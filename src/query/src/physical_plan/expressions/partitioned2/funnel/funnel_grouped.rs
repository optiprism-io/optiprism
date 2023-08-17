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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunnelResult {
    buckets: HashMap<i64, BucketResult, RandomState>,
}

impl FunnelResult {}

#[derive(Debug)]
pub struct Funnel {
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
    group_cols: Vec<Column>,
    groups: Vec<SortField>,
    row_converter: RowConverter,
    touch: Touch,
    partition_col: Column,
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
    buf: HashMap<usize, Batch, RandomState>,
    batch_id: usize,
    processed_batches: usize,
    debug: Vec<(usize, usize, DebugStep)>,
    buckets: Vec<i64>,
    result: Mutex<RefCell<FunnelResult>>,
    bucket_size: Duration,
    steps_completed: usize,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub ts_col: Column,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    group_cols: Vec<Column>,
    groups: Vec<SortField>,
    pub touch: Touch,
    pub partition_col: Column,
    pub bucket_size: Duration,
}

struct PartitionRow<'a> {
    row_id: usize,
    batch_id: usize,
    batch: &'a Batch,
}

impl Funnel {
    pub fn new(opts: Options) -> Self {
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

        let buckets = v
            .iter()
            .map(|v| {
                let b = BucketResult {
                    total_funnels: 0,
                    completed_funnels: 0,
                    steps: (0..opts.steps.len())
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
            group_cols: opts.group_cols,
            groups: opts.groups,
            row_converter: RowConverter::new(groups.clone())?,
            touch: opts.touch,
            partition_col: opts.partition_col,
            skip: false,
            skip_partition: 0,
            first: true,
            cur_partition: 0,
            partition_len: 0,
            partition_start: Row {
                row_id: 0,
                batch_id: 0,
            },
            const_row: None,
            cur_step: 0,
            steps: opts
                .steps
                .iter()
                .map(|_| Step {
                    ts: 0,
                    row_id: 0,
                    batch_id: 0,
                })
                .collect(),
            buf: Default::default(),
            batch_id: 0,
            processed_batches: 0,
            cur_row_id: 0,
            debug: Vec::with_capacity(100),
            buckets: v,
            result: Mutex::new(RefCell::new(FunnelResult { buckets })),
            bucket_size: opts.bucket_size,
            steps_completed: 0,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps_orders.len()
    }

    pub fn buckets(&self) -> Vec<i64> {
        self.buckets.clone()
    }

    pub fn schema(&self) -> SchemaRef {
        SchemaRef::new(Schema::new(self.fields()))
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

    pub fn push_result(&self) {
        if self.steps_completed == 0 {
            return;
        }
        let steps_completed = self.steps_completed;
        let is_completed = match &self.filter {
            // if no filter, then funnel is completed id all steps are completed
            None => steps_completed == self.steps_count(),
            Some(filter) => match filter {
                Filter::DropOffOnAnyStep => steps_completed != self.steps_count(),
                // drop off on defined step
                Filter::DropOffOnStep(drop_off_step_id) => steps_completed - 1 == *drop_off_step_id,
                // drop off if time to convert is out of range
                Filter::TimeToConvert(from, to) => {
                    if steps_completed != self.steps_count() {
                        false
                    } else {
                        let diff = self.steps[self.cur_step].ts - self.steps[0].ts;
                        from.num_milliseconds() <= diff && diff <= to.num_milliseconds()
                    }
                }
            },
        };

        println!("{:?}", self.steps);
        let mut ts = NaiveDateTime::from_timestamp_opt(self.steps[0].ts, 0).unwrap();
        let ts = ts.duration_trunc(self.bucket_size).unwrap();
        let k = ts.timestamp_millis();

        self.result
            .lock()
            .unwrap()
            .get_mut()
            .buckets
            .entry(k)
            .and_modify(|b| {
                b.total_funnels += 1;
                if is_completed {
                    b.completed_funnels += 1;
                }

                println!("{}", self.steps_completed);
                for idx in 0..self.steps_completed {
                    b.steps[idx].count += 1;
                    if idx > 0 {
                        b.steps[idx].total_time += (self.steps[idx - 1].ts - self.steps[0].ts);
                        b.steps[idx].total_time_from_start +=
                            (self.steps[idx].ts - self.steps[0].ts);
                    }
                }
            });
    }
}

impl PartitionedAggregateExpr for Funnel {
    fn group_columns(&self) -> Vec<Column> {
        vec![]
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

        let mut step_fields = self
            .steps
            .iter()
            .enumerate()
            .map(|(step_id, step)| {
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

        if self.first {
            self.first = false;
            self.partition_start = Row {
                row_id: 0,
                batch_id: self.batch_id,
            };
            self.cur_partition = partitions.value(0);
        }

        // clen up obsolete batches
        let mut to_remove = Vec::with_capacity(self.buf.len() - 1);
        // for (idx, batch) in &self.buf {
        // if batch.first_partition < self.cur_partition {
        // to_remove.push(idx.to_owned())
        // }
        // }

        for idx in to_remove {
            self.buf.remove(&idx);
        }
        let mut row_id = 0;
        let mut batch_id = self.batch_id;

        let groups = {
            let arrs = self
                .group_cols
                .iter()
                .map(|e| {
                    e.evaluate(batch)
                        .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
                })
                .collect::<result::Result<Vec<_>, _>>()?;

            self.row_converter.convert_columns(&arrs)?
        };

        // to bypass borrow checker

        loop {
            while !partition_exist.contains_key(&partitions.value(row_id))
                && row_id < batch.num_rows()
            {
                row_id += 1;
            }

            let mut batch = self.buf.get(&batch_id).unwrap();
            let cur_ts = batch.ts.value(row_id);
            if self.cur_step > 0 {
                if let Some(exclude) = &batch.exclude {
                    if !self.check_exclude(exclude, row_id) {
                        self.debug
                            .push((batch_id, row_id, DebugStep::ExcludeViolation));
                        self.steps[0] = self.steps[self.cur_step].clone();
                        self.cur_step = 0;
                        self.steps_completed = 0;

                        // continue, so this row will be processed twice, possible first step as well
                        continue;
                    }
                }

                if cur_ts - self.steps[0].ts > self.window.num_milliseconds() {
                    self.push_result();
                    self.debug.push((batch_id, row_id, DebugStep::OutOfWindow));
                    // self.cur_step = 0;
                    // self.steps_completed = 0;
                    // println!("?");
                    // row_id = self.steps[self.cur_step].row_id;
                    // batch_id = self.steps[self.cur_step].batch_id;
                    self.steps[0] = self.steps[self.cur_step].clone();
                    self.cur_step = 0;
                    self.steps_completed = 0;
                    // continue;
                }
            }
            println!("!");
            if self.cur_step == 0 {
                if batch.constants.is_some() {
                    self.const_row = Some(Row { row_id, batch_id })
                }
            } else {
                // compare current value with constant
                // get constant row
                if let Some(constants) = &batch.constants {
                    if !self.check_constants(&constants, row_id) {
                        self.debug
                            .push((batch_id, row_id, DebugStep::ConstantViolation));
                        self.steps[0] = self.steps[self.cur_step].clone();
                        self.cur_step = 0;
                        self.steps_completed = 0;

                        continue;
                    }
                }
            }

            if batch_id == self.batch_id && partitions.value(row_id) != self.cur_partition {
                self.push_result();
                self.debug.push((batch_id, row_id, DebugStep::NewPartition));
                self.cur_partition = partitions.value(row_id);
                self.partition_start = Row {
                    row_id: 0,
                    batch_id: self.batch_id,
                };
                self.cur_step = 0;
                self.skip = false;
                self.steps_completed = 0;
            }
            if !self.skip {
                let mut matched = false;
                match &self.steps_orders[self.cur_step] {
                    StepOrder::Sequential => {
                        matched = batch.steps[self.cur_step].value(row_id);
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
                    self.steps_completed += 1;

                    self.steps[self.cur_step].batch_id = batch_id;
                    self.steps[self.cur_step].row_id = row_id;
                    self.steps[self.cur_step].ts = cur_ts;
                    self.debug.push((batch_id, row_id, DebugStep::Step));
                    if self.cur_step < self.steps_count() - 1 {
                        self.cur_step += 1;
                    } else {
                        self.debug.push((batch_id, row_id, DebugStep::Complete));
                        // uncommit for single funnel per partition
                        // self.skip = true;

                        self.push_result();
                        self.cur_step = 0;
                        self.steps_completed = 0;
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
        self.push_result();
        // make it stateful so we can test it

        let buckets = self.result.lock().unwrap().get_mut().buckets.clone();
        let arr_len = buckets.len();
        let steps = self.steps.len();
        let ts =
            TimestampMillisecondArray::from(buckets.iter().map(|(k, _)| *k).collect::<Vec<_>>());
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
        for (k, b) in buckets {
            total.append_value(b.total_funnels);
            completed.append_value(b.completed_funnels);
            for (step_id, step) in b.steps.iter().enumerate() {
                step_total[step_id].append_value(step.count);
                let v = if step.count > 0 {
                    println!("ttt {} {}", step.total_time, step.total_time / step.count);
                    Decimal::from_i128_with_scale(step.total_time as i128, DECIMAL_SCALE as u32)
                        / Decimal::from_i128_with_scale(step.count as i128, DECIMAL_SCALE as u32)
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
                step_time_to_convert_from_start[step_id].append_value(v.mantissa() * 10000000000);
            }
        }

        let arrs: Vec<ArrayRef> = vec![
            Arc::new(ts),
            Arc::new(total.finish()),
            Arc::new(completed.finish()),
        ];

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

        let res = [arrs, step_arrs].concat();

        Ok(res)
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        let buckets = self
            .buckets
            .iter()
            .map(|v| {
                let b = BucketResult {
                    total_funnels: 0,
                    completed_funnels: 0,
                    steps: (0..self.steps.len())
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

        let res = Self {
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
            steps: self
                .steps
                .iter()
                .map(|_| Step {
                    ts: 0,
                    row_id: 0,
                    batch_id: 0,
                })
                .collect(),
            buf: Default::default(),
            batch_id: 0,
            processed_batches: 0,
            debug: vec![],
            buckets: self.buckets.clone(),
            result: Mutex::new(RefCell::new(FunnelResult { buckets })),
            bucket_size: self.bucket_size,
            steps_completed: 0,
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
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_grouped::DebugStep;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_grouped::Funnel;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_grouped::FunnelResult;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel_grouped::Options;
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
| 1      | 2020-04-12 22:13:57      | 1      | 1      |
| 1      | 2020-04-12 22:15:57      | 2      | 1      |
| 1      | 2020-04-12 22:17:57      | 3      | 1      |
| 2      | 2020-04-12 22:10:57      | 1      | 1      |
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
        };
        let mut f = Funnel::new(opts);
        for b in res {
            f.evaluate(&b, &hash).unwrap();
        }

        for (batch_id, row_id, op) in &f.debug {
            println!("batch: {} row: {} op: {:?} ", batch_id, row_id, op);
        }
        let res = f.finalize().unwrap();

        let b = RecordBatch::try_new(f.schema(), res).unwrap();
        print_batches(&[b]).unwrap();
    }
}
