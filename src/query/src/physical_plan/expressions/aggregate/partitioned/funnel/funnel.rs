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
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_row::SortField;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query::date_trunc;
use common::query::TimeIntervalUnit;
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use num_traits::FromPrimitive;
use num_traits::ToPrimitive;
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
    total: i64,
    dropped_off: i64,
    total_time_to_convert: i64,
    total_time_to_convert_from_start: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct BucketResult {
    steps: Vec<StepResult>,
    ts: i64,
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
    fn new(steps_len: usize, ts: &[i64]) -> Self {
        let buckets = ts
            .iter()
            .map(|ts| {
                let b = BucketResult {
                    steps: (0..steps_len)
                        .map(|_| StepResult {
                            total: 0,
                            dropped_off: 0,
                            total_time_to_convert: 0,
                            total_time_to_convert_from_start: 0,
                        })
                        .collect::<Vec<_>>(),
                    ts: *ts,
                };
                (*ts, b)
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
            // check if this exclude is relevant to current step
            if let Some(steps) = &excl.steps {
                if steps.from <= self.cur_step
                    && steps.to >= self.cur_step
                    && excl.exists.value(cur_row_id)
                {
                    return false;
                }
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

    pub fn push_result(
        &mut self,
        filter: &Option<Filter>,
        time_unit: &Option<TimeIntervalUnit>,
    ) -> bool {
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
        let k = if let Some(time_unit) = time_unit {
            let dt = chrono::DateTime::from_timestamp_millis(self.steps[0].ts).unwrap();
            let ts = date_trunc(time_unit, dt).unwrap();
            ts.timestamp_millis()
        } else {
            *self.buckets.iter().next().unwrap().0
        };

        // increment counters in bucket. Assume that bucket exist
        self.buckets.entry(k).and_modify(|b| {
            for idx in 0..self.steps_completed {
                b.steps[idx].total += 1;
                if idx > 0 {
                    b.steps[idx].total_time_to_convert +=
                        self.steps[idx].ts - self.steps[idx - 1].ts;
                    b.steps[idx].total_time_to_convert_from_start +=
                        self.steps[idx].ts - self.steps[0].ts;

                    b.steps[idx].dropped_off = b.steps[idx - 1].total - b.steps[idx].total;
                }
            }

            for idx in 1..self.steps.len() {
                b.steps[idx].dropped_off = b.steps[idx - 1].total - b.steps[idx].total;
            }
        });

        is_completed
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct Dbg {
    inner: Vec<(usize, usize, DebugStep)>,
}

impl Dbg {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            inner: Vec::with_capacity(100),
        }
    }
    #[allow(dead_code)]
    fn push(&mut self, batch_id: usize, row_id: usize, step: DebugStep) {
        self.inner.push((batch_id, row_id, step));
    }
}

#[derive(Debug)]
pub struct Funnel {
    input_schema: SchemaRef,
    ts_col: PhysicalExprRef,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps_orders: Vec<StepOrder>,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<PhysicalExprRef>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Option<Touch>,
    partition_col: PhysicalExprRef,
    buf: HashMap<usize, Batch, RandomState>,
    batch_id: usize,
    _processed_batches: usize,
    #[cfg(test)]
    debug: Dbg,
    buckets: Vec<i64>,
    time_interval: Option<TimeIntervalUnit>,
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
    pub ts_col: PhysicalExprRef,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub time_interval: Option<TimeIntervalUnit>,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<PhysicalExprRef>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Option<Touch>,
    pub partition_col: PhysicalExprRef,
    pub groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
}

impl Funnel {
    pub fn try_new(opts: Options) -> crate::error::Result<Self> {
        let buckets = if opts.time_interval.is_some() {
            let from = date_trunc(&opts.time_interval.clone().unwrap(), opts.from)?;
            let to = date_trunc(&opts.time_interval.clone().unwrap(), opts.to)?;
            let mut buckets = (from.timestamp_millis()..=to.timestamp_millis())
                .step_by(
                    opts.time_interval
                        .clone()
                        .unwrap()
                        .duration(1)
                        .num_milliseconds() as usize,
                )
                .collect::<Vec<i64>>();
            // case where bucket size is bigger than time range
            if buckets.is_empty() {
                buckets.push(from.timestamp_millis());
            }
            buckets
        } else {
            vec![opts.from.timestamp_millis()]
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
            _processed_batches: 0,
            #[cfg(test)]
            debug: Dbg::new(),
            buckets: buckets.clone(),
            time_interval: opts.time_interval.clone(),
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

    pub fn group_columns(&self) -> Vec<(PhysicalExprRef, String)> {
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

    pub fn fields(&self) -> Vec<Field> {
        let mut fields = vec![Field::new(
            "ts",
            DataType::Timestamp(TIME_UNIT, None),
            false,
        )];

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
                        format!("step{}_conversion_ratio", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                    Field::new(
                        format!("step{}_avg_time_to_convert", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                    Field::new(
                        format!("step{}_avg_time_to_convert_from_start", step_id),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                    Field::new(format!("step{step_id}_dropped_off"), DataType::Int64, false),
                    Field::new(
                        format!("step{step_id}_drop_off_ratio"),
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        false,
                    ),
                    Field::new(
                        format!("step{}_time_to_convert", step_id),
                        DataType::Int64,
                        true,
                    ),
                    Field::new(
                        format!("step{}_time_to_convert_from_start", step_id),
                        DataType::Int64,
                        true,
                    ),
                ];
                fields
            })
            .collect::<Vec<_>>();
        fields.append(&mut step_fields);

        fields
    }

    pub fn evaluate(
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
            .into_array(batch.num_rows())?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let rows = if let Some(groups) = &mut self.groups {
            let arrs = groups
                .exprs
                .iter()
                .map(|e| {
                    e.evaluate(batch)
                        .map(|v| v.into_array(batch.num_rows()).unwrap())
                })
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
                self.skip_partition = false;
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
                group.cur_partition = partitions.value(row_id);
            }

            let batch = self.buf.get(&batch_id).unwrap();
            let cur_ts = batch.ts.value(row_id);
            if group.cur_step > 0 {
                if let Some(exclude) = &batch.exclude {
                    if !group.check_exclude(exclude, row_id) {
                        #[cfg(test)]
                        self.debug
                            .push(batch_id, row_id, DebugStep::ExcludeViolation);
                        group.steps[0] = group.steps[group.cur_step].clone();
                        group.cur_step = 0;
                        group.steps_completed = 0;

                        // continue, so this row will be processed twice, possible first step as well
                        continue;
                    }
                }

                if cur_ts - group.steps[0].ts > self.window.num_milliseconds() {
                    group.push_result(&self.filter, &self.time_interval);
                    #[cfg(test)]
                    self.debug.push(batch_id, row_id, DebugStep::OutOfWindow);
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
                        #[cfg(test)]
                        self.debug
                            .push(batch_id, row_id, DebugStep::ConstantViolation);
                        group.steps[0] = group.steps[group.cur_step].clone();
                        group.cur_step = 0;
                        group.steps_completed = 0;

                        continue;
                    }
                }
            }

            if batch_id == self.batch_id && partitions.value(row_id) != group.cur_partition {
                group.push_result(&self.filter, &self.time_interval);
                #[cfg(test)]
                self.debug.push(batch_id, row_id, DebugStep::NewPartition);
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
            let mut matched = false;
            match &self.steps_orders[group.cur_step] {
                StepOrder::Exact => {
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
                #[cfg(test)]
                self.debug.push(batch_id, row_id, DebugStep::Step);
                if group.cur_step < self.steps_len - 1 {
                    group.cur_step += 1;
                } else {
                    #[cfg(test)]
                    self.debug.push(batch_id, row_id, DebugStep::Complete);

                    let is_completed = group.push_result(&self.filter, &self.time_interval);
                    if is_completed && self.count == Count::Unique {
                        self.skip_partition = true;
                    }
                    group.cur_step = 0;
                    group.steps_completed = 0;
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
            #[cfg(test)]
            self.debug.push(batch_id, row_id, DebugStep::NextRow);
        }

        self.batch_id += 1;

        Ok(())
    }

    pub fn make_new(&self) -> crate::Result<Funnel> {
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
            #[cfg(test)]
            debug: Dbg::new(),
            buckets: self.buckets.clone(),
            time_interval: self.time_interval.clone(),
            groups,
            cur_partition: 0,
            skip_partition: false,
            first: true,
            single_group: Group::new(self.steps_len, &self.buckets),
            steps_len: self.steps_len,
        };

        Ok(res)
    }

    pub fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        // in case of grouping make an array of groups (group_arrs)
        let (group_arrs, groups) = if let Some(groups) = &mut self.groups {
            let mut rows: Vec<arrow_row::Row> = Vec::with_capacity(groups.groups.len());
            for (row, group) in &mut groups.groups {
                rows.push(row.row());
                group.push_result(&self.filter, &self.time_interval);
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
                .push_result(&self.filter, &self.time_interval);
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

            // step total col
            let mut step_total = (0..steps)
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_conversion_ratio = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_avg_time_to_convert = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_avg_time_to_convert_from_start = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_dropped_off = (0..steps)
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_drop_off_ratio = (0..steps)
                .map(|_| Decimal128Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert = (0..steps)
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();
            let mut step_time_to_convert_from_start = (0..steps)
                .map(|_| Int64Builder::with_capacity(arr_len))
                .collect::<Vec<_>>();

            // iterate over buckets and fill values to builders
            for bucket in buckets.values() {
                for (step_id, step) in bucket.steps.iter().enumerate() {
                    step_total[step_id].append_value(step.total);
                    let mut scr = if step_id == 0 {
                        Decimal::from_f64(100.).unwrap()
                    } else {
                        Decimal::from_f64(if step.total > 0 {
                            step.total as f64 / bucket.steps[0].total as f64 * 100.
                        } else {
                            0.
                        })
                        .unwrap()
                    };
                    scr.rescale(DECIMAL_SCALE as u32);
                    step_conversion_ratio[step_id].append_value(scr.mantissa());
                    step_dropped_off[step_id].append_value(step.dropped_off);
                    let mut v = if step_id == 0 {
                        Decimal::from_f64(0.).unwrap()
                    } else {
                        Decimal::from_f64(if step.total > 0 {
                            step.dropped_off as f64 / step.total as f64 * 100.
                        } else {
                            0.
                        })
                        .unwrap()
                    };
                    v.rescale(DECIMAL_SCALE as u32);
                    let mut dor = Decimal::from_f64(100. - scr.to_f64().unwrap()).unwrap();
                    dor.rescale(DECIMAL_SCALE as u32);
                    step_drop_off_ratio[step_id].append_value(dor.mantissa());
                    let mut v = if step_id == 0 {
                        Decimal::from_f64(0.).unwrap()
                    } else {
                        Decimal::from_f64(if step.total > 0 {
                            step.total_time_to_convert as f64 / step.total as f64 * 100.
                        } else {
                            0.
                        })
                        .unwrap()
                    };
                    v.rescale(DECIMAL_SCALE as u32);
                    step_avg_time_to_convert[step_id].append_value(v.mantissa());

                    let mut v = if step_id == 0 {
                        Decimal::from_f64(0.).unwrap()
                    } else {
                        Decimal::from_f64(if step.total > 0 {
                            step.total_time_to_convert_from_start as f64 / step.total as f64 * 100.
                        } else {
                            0.
                        })
                        .unwrap()
                    };
                    v.rescale(DECIMAL_SCALE as u32);
                    step_avg_time_to_convert_from_start[step_id].append_value(v.mantissa());

                    step_time_to_convert[step_id].append_value(step.total_time_to_convert);
                    step_time_to_convert_from_start[step_id]
                        .append_value(step.total_time_to_convert_from_start);
                }
            }

            let mut arrs: Vec<ArrayRef> = vec![Arc::new(ts)];
            // make groups
            if let Some(g) = &group_arrs {
                let garr = g
                    .iter()
                    .map(|arr| {
                        // make scalar value from group and stretch it to array size of buckets len
                        ScalarValue::try_from_array(arr.as_ref(), group_id)
                            .map(|v| v.to_array_of_size(arr_len).unwrap())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                arrs = [garr, arrs].concat();
            }

            let step_arrs = (0..steps)
                .flat_map(|idx| {
                    let arrs = vec![
                        Arc::new(step_total[idx].finish()) as ArrayRef,
                        Arc::new(
                            step_conversion_ratio[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                        Arc::new(
                            step_avg_time_to_convert[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                        Arc::new(
                            step_avg_time_to_convert_from_start[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                        Arc::new(step_dropped_off[idx].finish()) as ArrayRef,
                        Arc::new(
                            step_drop_off_ratio[idx]
                                .finish()
                                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                                .unwrap(),
                        ) as ArrayRef,
                        Arc::new(step_time_to_convert[idx].finish()) as ArrayRef,
                        Arc::new(step_time_to_convert_from_start[idx].finish()) as ArrayRef,
                    ];
                    arrs
                })
                .collect::<Vec<_>>();
            res.push([arrs, step_arrs].concat());
        }

        if res.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema()).columns().to_vec());
        }
        let res = (0..res[0].len())
            .map(|col_id| {
                let arrs = res.iter().map(|v| v[col_id].as_ref()).collect::<Vec<_>>();
                concat(&arrs).unwrap()
            })
            .collect::<Vec<_>>();

        let groups_len = if let Some(g) = &self.groups {
            g.exprs.len()
        } else {
            0
        };

        let step0_total = res[groups_len + 1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut idx_arr = Int64Builder::new();
        for (idx, v) in step0_total.iter().enumerate() {
            if v.unwrap() != 0 {
                idx_arr.append_value(idx as i64);
            }
        }
        let a = Arc::new(idx_arr.finish()) as ArrayRef;
        let res = res
            .iter()
            .map(|c| take(&c, &a, None).unwrap())
            .collect::<Vec<_>>();
        Ok(res)
    }
}

#[allow(unused_attributes)]
#[ignore]
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
    use common::query::TimeIntervalUnit;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use storage::test_util::parse_markdown_tables;
    use tracing_test::traced_test;

    use crate::event_eq;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::event_eq_;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::DebugStep;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::StepOrder::Exact;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::NonUnique;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Filter;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder::Any;

    #[derive(Debug, Clone)]
    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        exp_debug: Vec<(usize, usize, DebugStep)>,
        partition_exist: HashMap<i64, (), RandomState>,
        _exp: &'static str,
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
| 1            | 1976-01-01 11:10:00      | e1          | 1          |
| 1            | 1976-01-01 11:12:00      | e2          | 1          |
| 1            | 1976-01-01 11:13:00      | e3          | 1          |
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
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::minutes(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                    time_interval: None,
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
                _exp: r#"
asd
                "#,
            },
            TestCase {
                name: "unique count should skip second funnel".to_string(),
                data: r#"
| user_id(i64) | ts(ts) | event(utf8) | const(i64) |
|--------------|--------|-------------|------------|
| 1            | 1      | e1          | 1          |
| 1            | 2      | e2          | 1          |
| 1            | 3      | e3          | 1          |
| 1            | 4      | e1          | 1          |
| 1            | 5      | e2          | 1          |
| 1            | 6      | e3          | 1          |
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
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::minutes(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                    time_interval: None,
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::Step),
                    (0, 2, DebugStep::Complete),
                    (0, 3, DebugStep::NextRow),
                ],
                partition_exist: HashMap::from_iter([(1, ()), (2, ())]),
                _exp: r#"
asd
                "#,
            },
            TestCase {
                name: "non-unique count should take both funnels".to_string(),
                data: r#"
| user_id(i64) | ts(ts) | event(utf8) | const(i64) |
|--------------|--------|-------------|------------|
| 1            | 1      | e1          | 1          |
| 1            | 2      | e2          | 1          |
| 1            | 3      | e3          | 1          |
| 1            | 4      | e1          | 1          |
| 1            | 5      | e2          | 1          |
| 1            | 6      | e3          | 1          |
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
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::minutes(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::NonUnique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                    time_interval: None,
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::Step),
                    (0, 2, DebugStep::Complete),
                    (0, 3, DebugStep::NextRow),
                    (0, 3, DebugStep::Step),
                    (0, 4, DebugStep::NextRow),
                    (0, 4, DebugStep::Step),
                    (0, 5, DebugStep::NextRow),
                    (0, 5, DebugStep::Step),
                    (0, 5, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ()), (2, ())]),
                _exp: r#"
asd
                "#,
            },
            TestCase {
                name: "3 steps in a row, 2 batches should pass".to_string(),
                data: r#"
| user_id(i64) | ts(ts) | event(utf8) | const(i64) |
|--------------|--------|-------------|------------|
| 1            | 0      | e1          | 1          |
|||||
| 1            | 1      | e2          | 1          |
| 1            | 2      | e3          | 1          |
"#,

                opts: Options {
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    schema: schema.clone(),
                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                    time_interval: None,
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (1, 0, DebugStep::Step),
                    (1, 1, DebugStep::NextRow),
                    (1, 1, DebugStep::Step),
                    (1, 1, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 2      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 3, DebugStep::NextRow),
                    (0, 3, DebugStep::Step),
                    (0, 3, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps with same constant should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 2      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: Some(vec![Arc::new(
                        Column::new_with_schema("const", &schema).unwrap(),
                    )]),
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
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
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps with different constant on second step should fail".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 2          |
            | 1            | 2      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: Some(vec![Arc::new(
                        Column::new_with_schema("const", &schema).unwrap(),
                    )]),
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::ConstantViolation),
                    (0, 2, DebugStep::NextRow),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps with different constant on second step should continue and pass"
                    .to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 2          |
            | 1            | 2      | e1          | 3          |
            | 1            | 3      | e2          | 3          |
            | 1            | 4      | e3          | 3          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(4, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: Some(vec![Arc::new(
                        Column::new_with_schema("const", &schema).unwrap(),
                    )]),
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::ConstantViolation),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::Step),
                    (0, 3, DebugStep::NextRow),
                    (0, 3, DebugStep::Step),
                    (0, 4, DebugStep::NextRow),
                    (0, 4, DebugStep::Step),
                    (0, 4, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps with exclude should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 1      | e4          | 1          |
            | 1            | 2      | e3          | 1          |
            | 1            | 3      | e1          | 1          |
            | 1            | 4      | e2          | 1          |
            | 1            | 5      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(5, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: Some(vec![ExcludeExpr {
                        expr: {
                            let l = Column::new_with_schema("event", &schema).unwrap();
                            let r = Literal::new(ScalarValue::Utf8(Some("e4".to_string())));
                            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
                            Arc::new(expr) as PhysicalExprRef
                        },
                        steps: None,
                    }]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::ExcludeViolation),
                    (0, 3, DebugStep::NextRow),
                    (0, 4, DebugStep::NextRow),
                    (0, 4, DebugStep::Step),
                    (0, 5, DebugStep::NextRow),
                    (0, 5, DebugStep::Step),
                    (0, 6, DebugStep::NextRow),
                    (0, 6, DebugStep::Step),
                    (0, 6, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
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
            // schema: schema.clone(),
            // from: DateTime::from_timestamp(0, 0).unwrap(),
            // to: DateTime::from_timestamp(8, 0).unwrap(),
            // bucket_size: TimeIntervalUnit::Hour,
            // groups: None,
            // ts_col: Arc::new(Column::new("ts", 1)),
            // window: Duration::milliseconds(3),
            // steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
            // exclude: None,
            // constants: None,
            // count: Count::Unique,
            // filter: None,
            // touch: None,
            // partition_col: Arc::new(Column::new("user_id", 0)),
            // },
            // exp_debug: vec![
            // (0, 0, DebugStep::Step),
            // (0, 1, DebugStep::NextRow),
            // (0, 1, DebugStep::Step),
            // (1, 0, DebugStep::OutOfWindow),
            // (0, 1, DebugStep::NextRow),
            // (1, 0, DebugStep::NextRow),
            // (1, 0, DebugStep::NextRow),
            // (1, 1, DebugStep::Step),
            // (1, 2, DebugStep::NextRow),
            // (1, 2, DebugStep::Step),
            // (1, 3, DebugStep::NextRow),
            // (1, 3, DebugStep::Step),
            // (1, 3, DebugStep::Complete),
            // ],
            // partition_exist: HashMap::from_iter([(1, ())]),
            // exp: r#"
            // asd
            // "#,
            // },
            TestCase {
                name: "3 steps in any order between 1-2 should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e3          | 1          |
            | 1            | 2      | e2          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq_(&schema, "e1", Exact),
                        event_eq_(&schema, "e2", Any(vec![(0, 2)])),
                        event_eq_(&schema, "e3", Any(vec![(1, 2)])),
                    ],
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
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
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "3 steps in a row should pass, 2 partitions".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 2      | e3          | 1          |
            | 2            | 0      | e1          | 1          |
            | 2            | 1      | e2          | 1          |
            | 2            | 2      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                    (0, 2, DebugStep::Step),
                    (0, 2, DebugStep::Complete),
                    (0, 3, DebugStep::NextRow),
                    (0, 3, DebugStep::NewPartition),
                    (0, 3, DebugStep::Step),
                    (0, 4, DebugStep::NextRow),
                    (0, 4, DebugStep::Step),
                    (0, 5, DebugStep::NextRow),
                    (0, 5, DebugStep::Step),
                    (0, 5, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ()), (2, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "2 partition. First fails, second pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e1          | 1          |
            | 1            | 2      | e1          | 1          |
            | 2            | 0      | e1          | 1          |
            | 2            | 1      | e2          | 1          |
            | 2            | 2      | e3          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 2, DebugStep::NextRow),
                    (0, 3, DebugStep::NextRow),
                    (0, 3, DebugStep::NewPartition),
                    (0, 3, DebugStep::Step),
                    (0, 4, DebugStep::NextRow),
                    (0, 4, DebugStep::Step),
                    (0, 5, DebugStep::NextRow),
                    (0, 5, DebugStep::Step),
                    (0, 5, DebugStep::Complete),
                ],
                partition_exist: HashMap::from_iter([(1, ()), (2, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "dropoff on any should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 2      | e4          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnAnyStep),
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
            TestCase {
                name: "dropoff on second should pass".to_string(),
                data: r#"
            | user_id(i64) | ts(ts) | event(utf8) | const(i64) |
            |--------------|--------|-------------|------------|
            | 1            | 0      | e1          | 1          |
            | 1            | 1      | e2          | 1          |
            | 1            | 2      | e4          | 1          |
            "#,

                opts: Options {
                    schema: schema.clone(),
                    from: DateTime::from_timestamp(0, 0).unwrap(),
                    to: DateTime::from_timestamp(2, 0).unwrap(),
                    time_interval: None,

                    groups: None,
                    ts_col: Arc::new(Column::new("ts", 1)),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Exact, "e2" Exact, "e3" Exact),
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnStep(2)),
                    touch: None,
                    partition_col: Arc::new(Column::new("user_id", 0)),
                },
                exp_debug: vec![
                    (0, 0, DebugStep::Step),
                    (0, 1, DebugStep::NextRow),
                    (0, 1, DebugStep::Step),
                    (0, 2, DebugStep::NextRow),
                ],
                partition_exist: HashMap::from_iter([(1, ())]),
                _exp: r#"
            asd
            "#,
            },
        ];

        // let run_only: Option<&str> = Some("non-unique count should take both funnels");
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
            let _res = pretty_format_batches(&[rb]).unwrap();
            assert_eq!(f.debug.inner, case.exp_debug);
            // assert_eq!(format!("{}", res), case.exp);
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
| 1      | 2020-04-12 22:10:57      | 1      | 1      |
| 1      | 2020-04-12 22:11:57      | 2      | 1      |
| 1      | 2020-04-12 22:12:57      | 3      | 1      |
| 1      | 2020-04-12 22:10:57      | 1      | 1      |
| 1      | 2020-04-12 22:11:57      | 2      | 1      |

"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (2, ()), (3, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };

        let _ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let opts = Options {
            schema: schema.clone(),
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::seconds(200),
            steps: vec![e1, e2, e3],
            exclude: None,
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: NonUnique,
            filter: None,
            touch: None,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: None,

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
    fn test_1min_buckets() {
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
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let opts = Options {
            schema: schema.clone(),
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
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
            touch: None,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: None,

            groups: None,
        };
        let mut f = Funnel::try_new(opts).unwrap();
        for b in res {
            f.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();

        // for (_, _, ds) in f.debug {
        //     println!("{ds:?}");
        // }
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
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
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
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
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
            touch: None,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: Some(TimeIntervalUnit::Day),
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
| 1      | 2020-04-12 01:10:57 | iphone       | 1      | 1      |
| 1      | 2020-04-12 02:11:57 | iphone       | 2      | 1      |
| 1      | 2020-04-12 03:12:57 | iphone       | 3      | 1      |
|        |                     |              |        |        |
| 1      | 2020-04-12 04:13:57 | android      | 1      | 1      |
| 1      | 2020-04-12 05:14:57 | android      | 2      | 1      |
| 1      | 2020-04-12 06:15:57 | android      | 3      | 1      |
| 3      | 2020-04-12 07:16:57 | android      | 1      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(1, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
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
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            from: DateTime::parse_from_str("2020-04-12 01:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 07:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::hours(4),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: NonUnique,
            filter: None,
            touch: None,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: Some(TimeIntervalUnit::Hour),
            // time_interval: None,
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
    fn test_all() {
        let data = r#"
| u(i64) | ts(ts)              | v(i64) | c(i64) |
|--------|---------------------|--------|--------|
| 1      | 2020-04-12 01:10:57 | 1      | 1      |
| 1      | 2020-04-12 02:11:57 | 2      | 1      |
| 1      | 2020-04-12 03:12:57 | 3      | 1      |
| 1      | 2020-04-12 01:10:57 | 1      | 1      |
| 1      | 2020-04-12 02:11:57 | 2      | 1      |
| 1      | 2020-04-12 03:12:57 | 3      | 1      |

"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(1, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let groups = vec![(
            Arc::new(Column::new_with_schema("c", &schema).unwrap()) as PhysicalExprRef,
            "device".to_string(),
            SortField::new(DataType::Int64),
        )];
        let opts = Options {
            schema: schema.clone(),
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            from: DateTime::parse_from_str("2020-04-12 01:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 07:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::hours(4),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: NonUnique,
            filter: None,
            touch: None,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: Some(TimeIntervalUnit::Hour),
            // time_interval: None,
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
