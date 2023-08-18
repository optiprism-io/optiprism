use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Builder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::concat;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use arrow_row::OwnedRow;
use arrow_row::RowConverter;
use arrow_row::SortField;
use chrono::DateTime;
use chrono::Duration;
use chrono::DurationRound;
use chrono::Months;
use chrono::NaiveDateTime;
use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use rust_decimal::Decimal;

use crate::error::QueryError;
use crate::physical_plan::expressions::partitioned2::funnel::evaluate_batch;
use crate::physical_plan::expressions::partitioned2::funnel::Batch;
use crate::physical_plan::expressions::partitioned2::funnel::Count;
use crate::physical_plan::expressions::partitioned2::funnel::Exclude;
use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
use crate::physical_plan::expressions::partitioned2::funnel::Filter;
use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
use crate::physical_plan::expressions::partitioned2::funnel::Touch;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;
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
    batch_id: usize,
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
    skip_partition: i64,
    first: bool,
    cur_partition: i64,
    cur_row_id: i64,
    partition_start: Row,
    partition_len: i64,
    const_row: Option<Row>,
    cur_step: usize,
    steps: Vec<Step>,
    buckets: HashMap<i64, BucketResult, RandomState>,
    steps_completed: usize,
}

impl Group {
    fn new(steps_len: usize, buckets: &[i64]) -> Self {
        let buckets = buckets
            .iter()
            .map(|v| {
                let b = BucketResult {
                    total_funnels: 0,
                    completed_funnels: 0,
                    steps: (0..steps_len)
                        .into_iter()
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
            skip_partition: 0,
            first: true,
            cur_partition: 0,
            cur_row_id: 0,
            partition_start: Row {
                row_id: 0,
                batch_id: 0,
            },
            partition_len: 0,
            const_row: None,
            cur_step: 0,
            steps: (0..steps_len)
                .into_iter()
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

    fn check_exclude(&self, exclude: &Vec<Exclude>, cur_row_id: usize) -> bool {
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

            if to_check {
                if excl.exists.value(cur_row_id) {
                    return false;
                }
            }
        }

        true
    }

    fn check_constants(&self, constants: &Vec<StaticArray>, cur_row_id: usize) -> bool {
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

    pub fn push_result(&mut self, filter: &Option<Filter>, bucket_size: Duration) {
        if self.steps_completed == 0 {
            return;
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

        let mut ts = NaiveDateTime::from_timestamp_opt(self.steps[0].ts, 0).unwrap();
        let ts = ts.duration_trunc(bucket_size).unwrap();
        let k = ts.timestamp_millis();

        self.buckets.entry(k).and_modify(|b| {
            b.total_funnels += 1;
            if is_completed {
                b.completed_funnels += 1;
            }

            for idx in 0..self.steps_completed {
                b.steps[idx].count += 1;
                if idx > 0 {
                    b.steps[idx].total_time += (self.steps[idx - 1].ts - self.steps[0].ts);
                    b.steps[idx].total_time_from_start += (self.steps[idx].ts - self.steps[0].ts);
                }
            }
        });
    }
}

#[derive(Debug)]
struct Groups {
    columns: Vec<Column>,
    sort_fields: Vec<SortField>,
    row_converter: RowConverter,
    groups: HashMap<OwnedRow, Group, RandomState>,
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
    processed_batches: usize,
    debug: Vec<(usize, usize, DebugStep)>,
    buckets: Vec<i64>,
    bucket_size: Duration,
    groups: Option<Groups>,
    single_group: Group,
    steps_len: usize,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub schema: SchemaRef,
    pub ts_col: Column,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub partition_col: Column,
    pub bucket_size: Duration,
    groups: Option<(Vec<(Column, SortField)>)>,
}

struct PartitionRow<'a> {
    row_id: usize,
    batch_id: usize,
    batch: &'a Batch,
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
        let v = (from..to)
            .into_iter()
            .step_by(opts.bucket_size.num_milliseconds() as usize)
            .collect::<Vec<i64>>();

        let groups = if let Some(pairs) = opts.groups {
            Some(Groups {
                columns: pairs.iter().map(|(c, _)| c.clone()).collect(),
                sort_fields: pairs.iter().map(|(_, s)| s.clone()).collect(),
                row_converter: RowConverter::new(pairs.iter().map(|(_, s)| s.clone()).collect())?,
                groups: Default::default(),
            })
        } else {
            None
        };
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
            processed_batches: 0,
            debug: Vec::with_capacity(100),
            buckets: v.clone(),
            bucket_size: opts.bucket_size,
            groups,
            single_group: Group::new(opts.steps.len(), &v),
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
    fn group_columns(&self) -> Vec<Column> {
        if let Some(groups) = &self.groups {
            groups.columns.clone()
        } else {
            vec![]
        }
    }

    fn fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("total", DataType::Int64, false),
            Field::new("completed", DataType::Int64, false),
        ];

        if let Some(groups) = &self.groups {
            let group_fields = groups
                .columns
                .iter()
                .map(|c| {
                    Field::new(
                        c.name(),
                        c.data_type(&self.input_schema).unwrap(),
                        c.nullable(&self.input_schema).unwrap(),
                    )
                })
                .collect::<Vec<_>>();

            fields = [group_fields, fields].concat();
        }

        let mut step_fields = (0..self.steps_len)
            .into_iter()
            .map(|step_id| {
                let fields = vec![
                    Field::new(format!("step{}_total", step_id), DataType::Int64, false),
                    Field::new(
                        format!("step{}_time_to_convert", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                    Field::new(
                        format!("step{}_time_to_convert_from_start", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                ];
                fields
            })
            .flatten()
            .collect::<Vec<_>>();
        fields.append(&mut step_fields);

        fields
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: &HashMap<i64, ()>,
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
                .columns
                .iter()
                .map(|e| {
                    e.evaluate(batch)
                        .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
                })
                .collect::<result::Result<Vec<_>, _>>()?;

            Some(groups.row_converter.convert_columns(&arrs)?)
        } else {
            None
        };

        // clen up obsolete batches
        // let mut to_remove = Vec::with_capacity(self.buf.len() - 1);
        // for (idx, batch) in &self.buf {
        //     if batch.first_partition < self.cur_partition {
        //         to_remove.push(idx.to_owned())
        //     }
        // }

        // for idx in to_remove {
        //     self.buf.remove(&idx);
        // }
        let mut row_id = 0;
        let mut batch_id = self.batch_id;

        loop {
            while !partition_exist.contains_key(&partitions.value(row_id))
                && row_id < batch.num_rows()
            {
                row_id += 1;
            }

            let group = if let Some(groups) = &mut self.groups {
                groups
                    .groups
                    .entry(rows.as_ref().unwrap().row(row_id).owned())
                    .or_insert_with(|| {
                        let mut group = Group::new(self.steps_len, &self.buckets);
                        group
                    })
            } else {
                &mut self.single_group
            };

            if group.first {
                group.first = false;
                group.partition_start = Row {
                    row_id: 0,
                    batch_id: self.batch_id,
                };
                group.cur_partition = partitions.value(0);
            }

            let mut batch = self.buf.get(&batch_id).unwrap();
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
                    group.push_result(&self.filter, self.bucket_size.clone());
                    self.debug.push((batch_id, row_id, DebugStep::OutOfWindow));
                    group.steps[0] = group.steps[group.cur_step].clone();
                    group.cur_step = 0;
                    group.steps_completed = 0;
                    // don't continue
                }
            }
            if group.cur_step == 0 {
                if batch.constants.is_some() {
                    group.const_row = Some(Row { row_id, batch_id })
                }
            } else {
                // compare current value with constant
                // get constant row
                if let Some(constants) = &batch.constants {
                    if !group.check_constants(&constants, row_id) {
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
                group.push_result(&self.filter, self.bucket_size.clone());
                self.debug.push((batch_id, row_id, DebugStep::NewPartition));
                group.cur_partition = partitions.value(row_id);
                group.partition_start = Row {
                    row_id: 0,
                    batch_id: self.batch_id,
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

                        group.push_result(&self.filter, self.bucket_size.clone());
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
                batch = self.buf.get(&batch_id).unwrap();
            }
            self.debug.push((batch_id, row_id, DebugStep::NextRow));
        }

        self.batch_id += 1;

        Ok(())
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        let (group_arrs, groups) = if let Some(groups) = &mut self.groups {
            let mut rows: Vec<arrow_row::Row> = Vec::with_capacity(groups.groups.len());
            for (row, group) in &mut groups.groups {
                rows.push(row.row());
                group.push_result(&self.filter, self.bucket_size.clone());
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
                .push_result(&self.filter, self.bucket_size.clone());
            (None, vec![&self.single_group.buckets])
        };

        println!("111 {:?}", group_arrs);
        let mut res = vec![];

        for (group_id, buckets) in groups.into_iter().enumerate() {
            let arr_len = buckets.len();
            let steps = self.steps_len;
            let ts = TimestampMillisecondArray::from(
                buckets.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            );
            let mut total = Int64Builder::with_capacity(arr_len);
            let mut completed = Int64Builder::with_capacity(arr_len);
            let mut step_total = (0..steps)
                .into_iter()
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert = (0..steps)
                .into_iter()
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert_from_start = (0..steps)
                .into_iter()
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            for (_, bucket) in buckets {
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

                    step_time_to_convert[step_id].append_value(v.mantissa() * 10000000000); // fixme remove const
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
            if let Some(g) = &group_arrs {
                let garr = g
                    .iter()
                    .map(|arr| {
                        ScalarValue::try_from_array(arr.as_ref(), group_id)
                            .and_then(|v| Ok(v.to_array_of_size(buckets.len())))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                arrs = [garr, arrs].concat();
            }

            let step_arrs = (0..steps)
                .into_iter()
                .map(|idx| {
                    let mut arrs = vec![
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
                .flatten()
                .collect::<Vec<_>>();
            res.push([arrs, step_arrs].concat());
        }

        let res = (0..res[0].len())
            .into_iter()
            .map(|col_id| {
                let arrs = res.iter().map(|v| v[col_id].as_ref()).collect::<Vec<_>>();
                concat(&arrs).unwrap()
            })
            .collect::<Vec<_>>();

        Ok(res)
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        let groups = if let Some(groups) = &self.groups {
            Some(Groups {
                columns: groups.columns.clone(),
                sort_fields: groups.sort_fields.clone(),
                row_converter: RowConverter::new(groups.sort_fields.clone())?,
                groups: Default::default(),
            })
        } else {
            None
        };

        let res = Self {
            input_schema: self.input_schema.clone(),
            ts_col: self.ts_col.clone(),
            window: self.window.clone(),
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
            processed_batches: 0,
            debug: vec![],
            buckets: self.buckets.clone(),
            bucket_size: self.bucket_size,
            groups,
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

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
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
    use store::test_util::parse_markdown_table_v1;
    use store::test_util::parse_markdown_tables;
    use tracing_test::traced_test;

    use crate::event_eq;
    use crate::expected_debug;
    use crate::physical_plan::expressions::partitioned2::funnel::event_eq_;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_trends_mix::DebugStep;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_trends_mix::Funnel;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_trends_mix::Options;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_trends_mix::StepResult;
    use crate::physical_plan::expressions::partitioned2::funnel::Count;
    use crate::physical_plan::expressions::partitioned2::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::partitioned2::funnel::Filter;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder::Any;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder::Sequential;
    use crate::physical_plan::expressions::partitioned2::funnel::Touch;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

    #[test]
    fn test() {
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
        let schema = res[0].schema().clone();
        let hash = HashMap::from([(0, ()), (1, ()), (2, ()), (3, ())]);

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
            f.evaluate(&b, &hash).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
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
        let schema = res[0].schema().clone();
        let hash = HashMap::from([(0, ()), (1, ()), (2, ()), (3, ())]);

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
            Column::new_with_schema("device", &schema).unwrap(),
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
            f.evaluate(&b, &hash).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
    }
}
