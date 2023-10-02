use std::collections::HashMap;
use std::result;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Builder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::concat;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use arrow_row::SortField;
use chrono::DateTime;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use rust_decimal::Decimal;

use crate::physical_plan::expressions::aggregate::partitioned::funnel::evaluate_batch;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Batch;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Exclude;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeExpr;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Filter;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::StaticArray;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DebugStep {
    NewPartition,
    Skip,
    Step,
    NextRow,
    ExcludeViolation,
    OutOfWindow,
    ConstantViolation,
    Complete,
    Incomplete,
}

#[derive(Debug, Clone)]
struct Step {
    ts: i64,
    row_id: usize,
    batch_id: usize,
}

#[derive(Debug)]
struct Row {
    row_id: usize,
    _batch_id: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct StepResult {
    count: i64,
    total_time: i64,
    total_time_from_start: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct BucketResult {
    total_funnels: i64,
    completed_funnels: i64,
    steps: Vec<StepResult>,
}

#[derive(Debug)]
struct Group {
    skip: bool,
    first: bool,
    cur_partition: i64,
    _cur_row_id: i64,
    partition_start: Row,
    _partition_len: i64,
    const_row: Option<Row>,
    cur_step: usize,
    steps: Vec<Step>,
    buckets: HashMap<i64, BucketResult, RandomState>,
    steps_completed: usize,
}

impl Group {
    // predefine buckets for each timestamp
    fn new(steps_len: usize, buckets: &[i64]) -> Self {
        let buckets = buckets
            .iter()
            .map(|v| {
                let b = BucketResult {
                    total_funnels: 0,
                    completed_funnels: 0,
                    steps: (0..steps_len)
                        .map(|_| StepResult {
                            count: 0,
                            total_time: 0,
                            total_time_from_start: 0,
                        })
                        .collect::<Vec<_>>(),
                };
                (*v, b)
            })
            .collect::<HashMap<_, _, RandomState>>();

        Self {
            skip: false,
            first: true,
            cur_partition: 0,
            _cur_row_id: 0,
            partition_start: Row {
                row_id: 0,
                _batch_id: 0,
            },
            _partition_len: 0,
            const_row: None,
            cur_step: 0,
            steps: (0..steps_len)
                .map(|_| Step {
                    ts: 0,
                    row_id: 0,
                    batch_id: 0,
                })
                .collect::<Vec<_>>(),
            buckets,
            steps_completed: 0,
        }
    }

    fn check_exclude(&self, exclude: &[Exclude], cur_row_id: usize) -> bool {
        for excl in exclude.iter() {
            let mut to_check = false;
            // check if this exclude is relevant to current step
            if let Some(steps) = &excl.steps {
                for pair in steps {
                    if pair.from <= self.cur_step && pair.to >= self.cur_step {
                        to_check = true;
                        break;
                    }
                }
            } else {
                // check anyway
                to_check = true;
            }

            if to_check && excl.exists.value(cur_row_id) {
                return false;
            }
        }

        true
    }

    fn check_constants(&self, constants: &[StaticArray], cur_row_id: usize) -> bool {
        let const_row = self.const_row.as_ref().unwrap();
        for (const_idx, first_const) in constants.iter().enumerate() {
            // compare the const values of current row and first row
            let cur_const = &constants[const_idx];
            if !first_const.eq_values(const_row.row_id, cur_const, cur_row_id) {
                return false;
            }
        }

        true
    }

    pub fn push_result(&mut self, filter: &Option<Filter>, bucket_size: Duration) -> bool {
        if self.steps_completed == 0 {
            return false;
        }
        let steps_count = self.steps.len();
        let steps_completed = self.steps_completed;
        let is_completed = match filter {
            // if no filter, then funnel is completed id all steps are completed
            None => steps_completed == steps_count,
            Some(filter) => match filter {
                Filter::DropOffOnAnyStep => steps_completed != steps_count,
                // drop off on defined step
                Filter::DropOffOnStep(drop_off_step_id) => steps_completed - 1 == *drop_off_step_id,
                // drop off if time to convert is out of range
                Filter::TimeToConvert(from, to) => {
                    if steps_completed != steps_count {
                        false
                    } else {
                        let diff = self.steps[self.cur_step].ts - self.steps[0].ts;
                        from.num_milliseconds() <= diff && diff <= to.num_milliseconds()
                    }
                }
            },
        };

        let ts = NaiveDateTime::from_timestamp_opt(self.steps[0].ts, 0).unwrap();
        let ts = ts.duration_trunc(bucket_size).unwrap();
        let k = ts.timestamp_millis();

        // increment counters in bucket. Assume that bucket exist
        self.buckets.entry(k).and_modify(|b| {
            b.total_funnels += 1;
            if is_completed {
                b.completed_funnels += 1;
            }

            for idx in 0..self.steps_completed {
                b.steps[idx].count += 1;
                if idx > 0 {
                    b.steps[idx].total_time += self.steps[idx - 1].ts - self.steps[0].ts;
                    b.steps[idx].total_time_from_start += self.steps[idx].ts - self.steps[0].ts;
                }
            }
        });

        is_completed
    }
}

#[derive(Debug)]
pub struct Funnel {
    input_schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps_orders: Vec<StepOrder>,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    partition_col: Column,
    buf: HashMap<usize, Batch, RandomState>,
    batch_id: usize,
    _processed_batches: usize,
    debug: Vec<(usize, usize, DebugStep)>,
    buckets: Vec<i64>,
    bucket_size: Duration,
    // groups when group by
    groups: Option<Groups<Group>>,
    cur_partition: i64,
    skip_partition: bool,
    first: bool,
    // special case for non-group
    single_group: Group,
    steps_len: usize,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub schema: SchemaRef,
    pub ts_col: Column,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub partition_col: Column,
    pub bucket_size: Duration,
    pub groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
}

impl Funnel {
    pub fn try_new(opts: Options) -> crate::error::Result<Self> {
        let from = opts
            .from
            .duration_trunc(opts.bucket_size)
            .unwrap()
            .timestamp_millis();
        let to = opts
            .to
            .duration_trunc(opts.bucket_size)
            .unwrap()
            .timestamp_millis();
        let mut buckets = (from..to)
            .step_by(opts.bucket_size.num_milliseconds() as usize)
            .collect::<Vec<i64>>();

        // case where bucket size is bigger than time range
        if buckets.is_empty() {
            buckets.push(from);
        }

        Ok(Self {
            input_schema: opts.schema,
            ts_col: opts.ts_col,
            window: opts.window,
            steps_expr: opts
                .steps
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>(),
            steps_orders: opts
                .steps
                .iter()
                .map(|(_, order)| order.clone())
                .collect::<Vec<_>>(),
            exclude_expr: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            partition_col: opts.partition_col,
            buf: Default::default(),
            batch_id: 0,
            _processed_batches: 0,
            debug: Vec::with_capacity(100),
            buckets: buckets.clone(),
            bucket_size: opts.bucket_size,
            groups: Groups::maybe_from(opts.groups)?,
            cur_partition: 0,
            skip_partition: false,
            first: true,
            single_group: Group::new(opts.steps.len(), &buckets),
            steps_len: opts.steps.len(),
        })
    }

    pub fn steps_count(&self) -> usize {
        self.steps_orders.len()
    }

    pub fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(self.fields()))
    }
}

impl PartitionedAggregateExpr for Funnel {
    fn group_columns(&self) -> Vec<(PhysicalExprRef, String)> {
        if let Some(groups) = &self.groups {
            groups
                .exprs
                .iter()
                .zip(groups.names.iter())
                .map(|(a, b)| (a.clone(), b.clone()))
                .collect()
        } else {
            vec![]
        }
    }

    fn fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("total", DataType::Int64, true),
            Field::new("completed", DataType::Int64, true),
        ];

        // prepend group fields if we have grouping
        if let Some(groups) = &self.groups {
            let group_fields = groups
                .exprs
                .iter()
                .zip(groups.names.iter())
                .map(|(expr, name)| {
                    Field::new(
                        name,
                        expr.data_type(&self.input_schema).unwrap(),
                        expr.nullable(&self.input_schema).unwrap(),
                    )
                })
                .collect::<Vec<_>>();

            fields = [group_fields, fields].concat();
        }

        // add fields for each step
        let mut step_fields = (0..self.steps_len)
            .flat_map(|step_id| {
                let fields = vec![
                    Field::new(format!("step{}_total", step_id), DataType::Int64, true),
                    Field::new(
                        format!("step{}_time_to_convert", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        true,
                    ),
                    Field::new(
                        format!("step{}_time_to_convert_from_start", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        true,
                    ),
                ];
                fields
            })
            .collect::<Vec<_>>();
        fields.append(&mut step_fields);

        fields
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: Option<&HashMap<i64, (), RandomState>>,
    ) -> crate::Result<()> {
        let funnel_batch = evaluate_batch(
            batch.to_owned(),
            &self.steps_expr,
            &self.exclude_expr,
            &self.constants,
            &self.ts_col,
            &self.partition_col,
        )?;
        self.buf.insert(self.batch_id, funnel_batch);
        let partitions = self
            .partition_col
            .evaluate(batch)?
            .into_array(batch.num_rows())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();

        let rows = if let Some(groups) = &mut self.groups {
            let arrs = groups
                .exprs
                .iter()
                .map(|e| e.evaluate(batch).map(|v| v.into_array(batch.num_rows())))
                .collect::<result::Result<Vec<_>, _>>()?;

            Some(groups.row_converter.convert_columns(&arrs)?)
        } else {
            None
        };

        let mut to_remove = Vec::with_capacity(self.buf.len() - 1);
        for (idx, batch) in &self.buf {
            if batch.first_partition < self.cur_partition {
                to_remove.push(idx.to_owned())
            }
        }

        for idx in to_remove {
            self.buf.remove(&idx);
        }
        let mut row_id = 0;
        let mut batch_id = self.batch_id;

        loop {
            if self.skip_partition {
                while self.cur_partition == partitions.value(row_id)
                    && row_id < batch.num_rows() - 1
                {
                    row_id += 1;
                }
            }
            if let Some(exists) = partition_exist {
                while !exists.contains_key(&partitions.value(row_id))
                    && row_id < batch.num_rows() - 1
                {
                    row_id += 1;
                }
            }

            let group = if let Some(groups) = &mut self.groups {
                groups
                    .groups
                    .entry(rows.as_ref().unwrap().row(row_id).owned())
                    .or_insert_with(|| Group::new(self.steps_len, &self.buckets))
            } else {
                &mut self.single_group
            };
            if self.first {
                self.cur_partition = partitions.value(row_id);
            }

            if group.first {
                group.first = false;
                group.partition_start = Row {
                    row_id: 0,
                    _batch_id: self.batch_id,
                };
                group.cur_partition = partitions.value(0);
            }

            let batch = self.buf.get(&batch_id).unwrap();
            let cur_ts = batch.ts.value(row_id);
            if group.cur_step > 0 {
                if let Some(exclude) = &batch.exclude {
                    if !group.check_exclude(exclude, row_id) {
                        self.debug
                            .push((batch_id, row_id, DebugStep::ExcludeViolation));
                        group.steps[0] = group.steps[group.cur_step].clone();
                        group.cur_step = 0;
                        group.steps_completed = 0;

                        // continue, so this row will be processed twice, possible first step as well
                        continue;
                    }
                }

                if cur_ts - group.steps[0].ts > self.window.num_milliseconds() {
                    group.push_result(&self.filter, self.bucket_size);
                    self.debug.push((batch_id, row_id, DebugStep::OutOfWindow));
                    group.steps[0] = group.steps[group.cur_step].clone();
                    group.cur_step = 0;
                    group.steps_completed = 0;
                    // don't continue
                }
            }
            if group.cur_step == 0 {
                if batch.constants.is_some() {
                    group.const_row = Some(Row {
                        row_id,
                        _batch_id: batch_id,
                    })
                }
            } else {
                // compare current value with constant
                // get constant row
                if let Some(constants) = &batch.constants {
                    if !group.check_constants(constants, row_id) {
                        self.debug
                            .push((batch_id, row_id, DebugStep::ConstantViolation));
                        group.steps[0] = group.steps[group.cur_step].clone();
                        group.cur_step = 0;
                        group.steps_completed = 0;

                        continue;
                    }
                }
            }

            if batch_id == self.batch_id && partitions.value(row_id) != group.cur_partition {
                group.push_result(&self.filter, self.bucket_size);
                self.debug.push((batch_id, row_id, DebugStep::NewPartition));
                group.cur_partition = partitions.value(row_id);
                self.cur_partition = partitions.value(row_id);
                group.partition_start = Row {
                    row_id: 0,
                    _batch_id: self.batch_id,
                };
                group.cur_step = 0;
                group.skip = false;
                group.steps_completed = 0;
            }
            if !group.skip {
                let mut matched = false;
                match &self.steps_orders[group.cur_step] {
                    StepOrder::Sequential => {
                        matched = batch.steps[group.cur_step].value(row_id);
                    }
                    StepOrder::Any(pairs) => {
                        for (from, to) in pairs {
                            for step in *from..=*to {
                                matched = batch.steps[step].value(row_id);
                                if matched {
                                    break;
                                }
                            }
                        }
                    }
                }
                if matched {
                    group.steps_completed += 1;

                    group.steps[group.cur_step].batch_id = batch_id;
                    group.steps[group.cur_step].row_id = row_id;
                    group.steps[group.cur_step].ts = cur_ts;
                    self.debug.push((batch_id, row_id, DebugStep::Step));
                    if group.cur_step < self.steps_len - 1 {
                        group.cur_step += 1;
                    } else {
                        self.debug.push((batch_id, row_id, DebugStep::Complete));
                        // uncommit for single funnel per partition
                        // self.skip = true;

                        let is_completed = group.push_result(&self.filter, self.bucket_size);
                        if is_completed && self.count == Count::Unique {
                            self.skip_partition = true;
                        }
                        group.cur_step = 0;
                        group.steps_completed = 0;
                    }
                }
            }
            row_id += 1;
            if row_id >= batch.len() {
                batch_id += 1;
                row_id = 0;
                if batch_id > self.batch_id {
                    break;
                }
            }
            self.debug.push((batch_id, row_id, DebugStep::NextRow));
        }

        self.batch_id += 1;

        Ok(())
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        // in case of grouping make an array of groups (group_arrs)
        let (group_arrs, groups) = if let Some(groups) = &mut self.groups {
            let mut rows: Vec<arrow_row::Row> = Vec::with_capacity(groups.groups.len());
            for (row, group) in &mut groups.groups {
                rows.push(row.row());
                group.push_result(&self.filter, self.bucket_size);
            }
            let group_arrs = groups.row_converter.convert_rows(rows)?;

            let buckets = groups
                .groups
                .iter()
                .map(|g| &g.1.buckets)
                .collect::<Vec<_>>();

            (Some(group_arrs), buckets)
        } else {
            self.single_group
                .push_result(&self.filter, self.bucket_size);
            (None, vec![&self.single_group.buckets])
        };

        let mut res = vec![];

        // iterate over groups
        for (group_id, buckets) in groups.into_iter().enumerate() {
            let arr_len = buckets.len();
            let steps = self.steps_len;
            // timestamp col
            let ts = TimestampMillisecondArray::from(
                buckets.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            );
            // total col
            let mut total = Int64Builder::with_capacity(arr_len);
            // completed col
            let mut completed = Int64Builder::with_capacity(arr_len);
            // step total col
            let mut step_total = (0..steps)
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert_from_start = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();

            // iterate over buckets and fill values to builders
            for bucket in buckets.values() {
                total.append_value(bucket.total_funnels);
                completed.append_value(bucket.completed_funnels);
                for (step_id, step) in bucket.steps.iter().enumerate() {
                    step_total[step_id].append_value(step.count);
                    let v = if step.count > 0 {
                        Decimal::from_i128_with_scale(step.total_time as i128, DECIMAL_SCALE as u32)
                            / Decimal::from_i128_with_scale(
                                step.count as i128,
                                DECIMAL_SCALE as u32,
                            )
                    } else {
                        Decimal::from_i128_with_scale(0, 0)
                    };

                    step_time_to_convert[step_id].append_value(v.mantissa());
                    let v = if step.count > 0 {
                        Decimal::from_i128_with_scale(
                            step.total_time_from_start as i128,
                            DECIMAL_SCALE as u32,
                        ) / Decimal::from_i128_with_scale(step.count as i128, DECIMAL_SCALE as u32)
                    } else {
                        Decimal::from_i128_with_scale(0, 0)
                    };
                    step_time_to_convert_from_start[step_id]
                        .append_value(v.mantissa() * 10000000000);
                }
            }

            let mut arrs: Vec<ArrayRef> = vec![
                Arc::new(ts),
                Arc::new(total.finish()),
                Arc::new(completed.finish()),
            ];
            // make groups
            if let Some(g) = &group_arrs {
                let garr = g
                    .iter()
                    .map(|arr| {
                        // make scalar value from group and stretch it to array size of buckets len
                        ScalarValue::try_from_array(arr.as_ref(), group_id)
                            .map(|v| v.to_array_of_size(buckets.len()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                arrs = [garr, arrs].concat();
            }

            let step_arrs = (0..steps)
                .flat_map(|idx| {
                    let arrs = vec![
                        Arc::new(step_total[idx].finish()) as ArrayRef,
                        Arc::new(
                            step_time_to_convert[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                        Arc::new(
                            step_time_to_convert_from_start[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                    ];
                    arrs
                })
                .collect::<Vec<_>>();
            res.push([arrs, step_arrs].concat());
        }

        let res = (0..res[0].len())
            .map(|col_id| {
                let arrs = res.iter().map(|v| v[col_id].as_ref()).collect::<Vec<_>>();
                concat(&arrs).unwrap()
            })
            .collect::<Vec<_>>();

        Ok(res)
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        let groups = if let Some(groups) = &self.groups {
            Some(groups.try_make_new()?)
        } else {
            None
        };

        let res = Self {
            input_schema: self.input_schema.clone(),
            ts_col: self.ts_col.clone(),
            window: self.window,
            steps_expr: self.steps_expr.clone(),
            steps_orders: self.steps_orders.clone(),
            exclude_expr: self.exclude_expr.clone(),
            constants: self.constants.clone(),
            count: self.count.clone(),
            filter: self.filter.clone(),
            touch: self.touch.clone(),
            partition_col: self.partition_col.clone(),
            buf: Default::default(),
            batch_id: 0,
            _processed_batches: 0,
            debug: vec![],
            buckets: self.buckets.clone(),
            bucket_size: self.bucket_size,
            groups,
            cur_partition: 0,
            skip_partition: false,
            first: true,
            single_group: Group::new(self.steps_len, &self.buckets),
            steps_len: self.steps_len,
        };

        Ok(Box::new(res))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ahash::RandomState;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::pretty_format_batches;
    use arrow::util::pretty::print_batches;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;
    use tracing_test::traced_test;

    use crate::event_eq;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::event_eq_;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::DebugStep;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::StepOrder::Sequential;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;

    #[derive(Debug, Clone)]
    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        exp_debug: Vec<(usize, usize, DebugStep)>,
        partition_exist: HashMap<i64, (), RandomState>,
        exp: &'static str,
    }

    #[traced_test]
    #[test]
    fn test_cases() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("const", DataType::Int64, false),
        ])) as SchemaRef;

        let cases = vec![
            TestCase {
                name: "3 steps in a row should pass".to_string(),
                data: r#"
| user_id(i64) | ts(ts) | event(utf8) | const(i64) |
|--------------|--------|-------------|------------|
| 1            | 1976-01-01 12:00:00      | e1          | 1          |
| 1            | 1976-01-01 13:00:00      | e2          | 1          |
| 1            | 1976-01-01 14:00:00      | e3          | 1          |
"#,
                opts: Options {
                    from: DateTime::parse_from_str(
                        "1976-01-01 12:00:00 +0000",
                        "%Y-%m-%d %H:%M:%S %z",
                    )
                    .unwrap()
                    .with_timezone(&Utc),
                    to: DateTime::parse_from_str(
                        "1976-02-01 12:00:00 +0000",
                        "%Y-%m-%d %H:%M:%S %z",
                    )
                    .unwrap()
                    .with_timezone(&Utc),
                    schema: schema.clone(),
                    groups: None,
                    ts_col: Column::new("ts", 1),
                    window: Duration::minutes(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                    partition_col: Column::new("user_id", 0),
                    bucket_size: Duration::minutes(10),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::Step),
                    (0, 2, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                exp: r#"
asd
                "#,
            },
            // TestCase {
            // name: "3 steps in a row, 2 batches should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // |||||
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (1, 0, DebugStep::Step),
            // (1, 1, DebugStep::NextRow),
            // (1, 1, DebugStep::Step),
            // (1, 1, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // (0, 3, DebugStep::NextRow),
            // (0, 3, DebugStep::Step),
            // (0, 3, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps with same constant should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: Some(vec![Column::new_with_schema("const", &schema).unwrap()]),
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // (0, 2, DebugStep::Step),
            // (0, 2, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps with different constant on second step should fail".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 2          |
            // | 1            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: Some(vec![Column::new_with_schema("const", &schema).unwrap()]),
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::ConstantViolation),
            // (0, 2, DebugStep::NextRow),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps with different constant on second step should continue and pass"
            // .to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 2          |
            // | 1            | 2      | e1          | 3          |
            // | 1            | 3      | e2          | 3          |
            // | 1            | 4      | e3          | 3          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: Some(vec![Column::new_with_schema("const", &schema).unwrap()]),
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::ConstantViolation),
            // (0, 2, DebugStep::NextRow),
            // (0, 2, DebugStep::Step),
            // (0, 3, DebugStep::NextRow),
            // (0, 3, DebugStep::Step),
            // (0, 4, DebugStep::NextRow),
            // (0, 4, DebugStep::Step),
            // (0, 4, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps with exclude should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 1      | e4          | 1          |
            // | 1            | 2      | e3          | 1          |
            // | 1            | 3      | e1          | 1          |
            // | 1            | 4      | e2          | 1          |
            // | 1            | 5      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: Some(vec![ExcludeExpr {
            // expr: {
            // let l = Column::new_with_schema("event", &schema).unwrap();
            // let r = Literal::new(ScalarValue::Utf8(Some("e4".to_string())));
            // let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            // Arc::new(expr) as PhysicalExprRef
            // },
            // steps: None,
            // }]),
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // (0, 2, DebugStep::ExcludeViolation),
            // (0, 3, DebugStep::NextRow),
            // (0, 4, DebugStep::NextRow),
            // (0, 4, DebugStep::Step),
            // (0, 5, DebugStep::NextRow),
            // (0, 5, DebugStep::Step),
            // (0, 6, DebugStep::NextRow),
            // (0, 6, DebugStep::Step),
            // (0, 6, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps in a row with window should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // |||||
            // | 1            | 5      | e3          | 1          |
            // | 1            | 6      | e1          | 1          |
            // | 1            | 7      | e2          | 1          |
            // | 1            | 8      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::milliseconds(3),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (1, 0, DebugStep::OutOfWindow),
            // (0, 1, DebugStep::NextRow),
            // (1, 0, DebugStep::NextRow),
            // (1, 1, DebugStep::NextRow),
            // (1, 1, DebugStep::Step),
            // (1, 2, DebugStep::NextRow),
            // (1, 2, DebugStep::Step),
            // (1, 3, DebugStep::NextRow),
            // (1, 3, DebugStep::Step),
            // (1, 3, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps in any order between 1-2 should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e3          | 1          |
            // | 1            | 2      | e2          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: vec![
            // event_eq_(&schema, "e1", Sequential),
            // event_eq_(&schema, "e2", Any(vec![(0, 2)])),
            // event_eq_(&schema, "e3", Any(vec![(1, 2)])),
            // ],
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // (0, 2, DebugStep::Step),
            // (0, 2, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "3 steps in a row should pass, 2 partitions".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e3          | 1          |
            // | 2            | 0      | e1          | 1          |
            // | 2            | 1      | e2          | 1          |
            // | 2            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // (0, 2, DebugStep::Step),
            // (0, 2, DebugStep::Complete),
            // (0, 3, DebugStep::NextRow),
            // (0, 3, DebugStep::NewPartition),
            // (0, 3, DebugStep::Step),
            // (0, 4, DebugStep::NextRow),
            // (0, 4, DebugStep::Step),
            // (0, 5, DebugStep::NextRow),
            // (0, 5, DebugStep::Step),
            // (0, 5, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "2 partition. First fails, second pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e1          | 1          |
            // | 1            | 2      | e1          | 1          |
            // | 2            | 0      | e1          | 1          |
            // | 2            | 1      | e2          | 1          |
            // | 2            | 2      | e3          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 2, DebugStep::NextRow),
            // (0, 3, DebugStep::NextRow),
            // (0, 3, DebugStep::NewPartition),
            // (0, 3, DebugStep::Step),
            // (0, 4, DebugStep::NextRow),
            // (0, 4, DebugStep::Step),
            // (0, 5, DebugStep::NextRow),
            // (0, 5, DebugStep::Step),
            // (0, 5, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "dropoff on any should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e4          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: Some(Filter::DropOffOnAnyStep),
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "dropoff on second should pass".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e4          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: Some(Filter::DropOffOnStep(2)),
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: r#"
            // asd
            // "#
            // },
            // TestCase {
            // name: "dropoff on first step should fail".to_string(),
            // data: r#"
            // | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            // |--------------|--------|-------------|------------|
            // | 1            | 0      | e1          | 1          |
            // | 1            | 1      | e2          | 1          |
            // | 1            | 2      | e4          | 1          |
            // "#,
            //
            // opts: Options {
            // ts_col: Column::new("ts", 1),
            // window: Duration::seconds(15),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: Some(Filter::DropOffOnStep(0)),
            // touch: Touch::First,
            // partition_col: Column::new("user_id", 0),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (0, 2, DebugStep::NextRow),
            // ],
            // partition_exist: HashMap::from([(1, ())]),
            // exp: FunnelResult {
            // total_funnels: 1,
            // completed_funnels: 0,
            // steps: vec![
            // StepResult {
            // count: 1,
            // total_time: 0,
            // total_time_from_start: 0,
            // },
            // StepResult {
            // count: 1,
            // total_time: 1,
            // total_time_from_start: 1,
            // },
            // StepResult {
            // count: 0,
            // total_time: 0,
            // total_time_from_start: 0,
            // },
            // ],
            // },
            // },
        ];

        let run_only: Option<&str> = None;
        for case in cases.iter().cloned() {
            if let Some(name) = run_only {
                if case.name != name {
                    continue;
                }
            }
            println!("\ntest case : {}", case.name);
            println!("============================================================");
            let rbs = parse_markdown_tables(case.data).unwrap();

            let mut f = Funnel::try_new(case.opts).unwrap();

            for rb in rbs {
                f.evaluate(&rb, Some(&case.partition_exist))?;
            }
            let res = f.finalize()?;
            let rb = RecordBatch::try_new(f.schema(), res).unwrap();
            let res = pretty_format_batches(&[rb]).unwrap();
            assert_eq!(f.debug, case.exp_debug);
            assert_eq!(format!("{}", res), case.exp);
            println!("PASSED");
        }

        Ok(())
    }

    #[test]
    fn test3() {
        let data = r#"
| u(i64) | ts(ts) | v(i64) | c(i64) |
|--------|--------|--------|--------|
| 1      | 2020-04-12 22:10:57      | 1      | 1      |
| 1      | 2020-04-12 22:11:57      | 2      | 1      |
| 1      | 2020-04-12 22:12:57      | 3      | 1      |
| 1      | 2020-04-12 22:13:57      | 1      | 1      |
| 1      | 2020-04-12 22:14:57      | 2      | 1      |
| 1      | 2020-04-12 22:15:57      | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (2, ()), (3, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::milliseconds(200),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
            bucket_size: Duration::minutes(1),
            groups: None,
        };
        let mut f = Funnel::try_new(opts).unwrap();
        for b in res {
            f.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
    }

    #[test]
    fn test2() {
        let data = r#"
| u(i64) | ts(ts) | v(i64) | c(i64) |
|--------|--------|--------|--------|
| 1      | 2020-04-12 22:10:57      | 1      | 1      |
| 1      | 2020-04-12 22:11:57      | 2      | 1      |
| 1      | 2020-04-12 22:12:57      | 3      | 1      |
|||||
| 1      | 2020-04-12 22:13:57      | 1      | 1      |
| 1      | 2020-04-12 22:15:57      | 2      | 1      |
| 1      | 2020-04-12 22:17:57      | 3      | 1      |
|||||
| 2      | 2020-04-12 22:10:57      | 1      | 1      |
|||||
| 2      | 2020-04-12 22:11:57      | 2      | 1      |
| 2      | 2020-04-12 22:12:57      | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (2, ()), (3, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::seconds(100),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
            bucket_size: Duration::minutes(1),
            groups: None,
        };
        let mut f = Funnel::try_new(opts).unwrap();
        for b in res {
            f.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();

        for (_, _, ds) in f.debug {
            println!("{ds:?}");
        }
    }

    #[test]
    fn test_groups() {
        let data = r#"
| u(i64) | ts(ts)              | device(utf8) | v(i64) | c(i64) |
|--------|---------------------|--------------|--------|--------|
| 1      | 2020-04-12 22:10:57 | iphone       | 1      | 1      |
| 1      | 2020-04-12 22:11:57 | iphone       | 2      | 1      |
| 1      | 2020-04-12 22:12:57 | iphone       | 3      | 1      |
|        |                     |              |        |        |
| 1      | 2020-04-12 22:13:57 | android      | 1      | 1      |
| 1      | 2020-04-12 22:15:57 | android      | 2      | 1      |
| 1      | 2020-04-12 22:17:57 | android      | 3      | 1      |
|        |                     |              |        |        |
| 2      | 2020-04-12 22:10:57 | ios          | 1      | 1      |
|        |                     |              |        |        |
| 2      | 2020-04-12 22:11:57 | ios          | 2      | 1      |
| 2      | 2020-04-12 22:12:57 | ios          | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (2, ()), (3, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let groups = vec![(
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_string(),
            SortField::new(DataType::Utf8),
        )];
        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::milliseconds(200),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
            bucket_size: Duration::minutes(1),
            groups: Some(groups),
        };
        let mut f = Funnel::try_new(opts).unwrap();
        for b in res {
            f.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
    }

    #[test]
    fn test_groups2() {
        let data = r#"
| u(i64) | ts(ts)              | device(utf8) | v(i64) | c(i64) |
|--------|---------------------|--------------|--------|--------|
| 1      | 2020-04-12 22:10:57 | iphone       | 1      | 1      |
| 1      | 2020-04-12 22:11:57 | iphone       | 2      | 1      |
| 1      | 2020-04-12 22:12:57 | iphone       | 3      | 1      |
|        |                     |              |        |        |
| 1      | 2020-04-12 22:13:57 | android      | 1      | 1      |
| 1      | 2020-04-12 22:15:57 | android      | 2      | 1      |
| 1      | 2020-04-12 22:17:57 | android      | 3      | 1      |
| 3      | 2020-04-12 22:17:57 | android      | 1      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(1, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let groups = vec![(
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_string(),
            SortField::new(DataType::Utf8),
        )];
        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::milliseconds(1200),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
            bucket_size: Duration::days(1),
            groups: Some(groups),
        };
        let mut f = Funnel::try_new(opts).unwrap();
        for b in res {
            f.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
    }
}
