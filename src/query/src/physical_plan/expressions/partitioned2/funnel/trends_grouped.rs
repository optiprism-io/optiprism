use std::collections::HashMap;
use std::result;

use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;

use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

#[derive(Clone, Debug)]
struct BucketStep {
    count: i64,
    time_to_convert: i64,
    time_to_convert_from_start: i64,
}

impl BucketStep {
    pub fn new() -> Self {
        Self {
            count: 0,
            time_to_convert: 0,
            time_to_convert_from_start: 0,
        }
    }
}

#[derive(Clone, Debug)]
struct Bucket {
    total: i64,
    completed: i64,
    completed_steps: i64,
    steps: Vec<BucketStep>,
}

impl Bucket {
    pub fn new(steps: usize) -> Self {
        let steps = (0..steps).into_iter().map(|_| BucketStep::new()).collect();
        Self {
            total: 0,
            completed: 0,
            completed_steps: 0,
            steps,
        }
    }
}

#[derive(Clone, Debug)]
struct DateTimeBuckets {
    size: Duration,
    steps_count: usize,
    ts: Vec<i64>,
    buckets: HashMap<i64, Bucket>,
}

impl DateTimeBuckets {
    pub fn new(steps_count: usize, ts: Vec<i64>, size: Duration) -> Self {
        let mut buckets = HashMap::new();
        for k in ts.clone() {
            buckets.insert(k, Bucket::new(steps_count));
        }
        Self {
            size,
            steps_count,
            ts,
            buckets,
        }
    }
}

impl Buckets for DateTimeBuckets {
    fn push(&mut self, results: Vec<FunnelResult>) {
        let mut is_completed = false;
        let mut completed_steps = 0;
        let mut ts: NaiveDateTime;
        let mut result_steps = vec![];
        for result in results {
            match result {
                FunnelResult::Completed(steps) => {
                    ts = NaiveDateTime::from_timestamp_opt(steps[0].ts, 0).unwrap();
                    is_completed = true;
                    completed_steps = steps.len();
                    result_steps = steps.clone();
                }
                FunnelResult::Incomplete(steps, stepn) => {
                    ts = NaiveDateTime::from_timestamp_opt(steps[0].ts, 0).unwrap();
                    is_completed = false;
                    completed_steps = stepn;
                    result_steps = steps.clone();
                }
            }
            ts = ts.duration_trunc(self.size).unwrap();
            let k = ts.timestamp_millis();

            for (idx, step) in result_steps.iter().enumerate() {
                let time_to_convert = if idx == 0 {
                    0
                } else {
                    step.ts - result_steps[idx - 1].ts
                };
                let time_to_convert_from_start = if idx == 0 {
                    0
                } else {
                    step.ts - result_steps[0].ts
                };
                let bucket = self.buckets.get_mut(&k).unwrap();
                bucket.total += 1;
                bucket.completed_steps += completed_steps as i64;
                if is_completed {
                    bucket.completed += 1;
                }

                for step in 0..self.steps_count {
                    if step < completed_steps {
                        bucket.steps[step].count += 1;
                        bucket.steps[step].time_to_convert = time_to_convert;
                        bucket.steps[step].time_to_convert_from_start = time_to_convert_from_start;
                    }
                }
            }
        }
    }

    fn buckets(&self) -> Vec<i64> {
        self.ts.clone()
    }

    fn finalize(&self) -> Vec<ArrayRef> {
        let arr_len = self.buckets().len();
        let steps = self.steps_count;
        let ts = TimestampMillisecondArray::from(self.buckets());
        let mut total = Int64Builder::with_capacity(arr_len);
        let mut completed = Int64Builder::with_capacity(arr_len);
        let mut step_total = (0..steps)
            .into_iter()
            .map(|_| Int64Builder::with_capacity(arr_len))
            .collect::<Vec<_>>();
        let mut step_time_to_convert = (0..steps)
            .into_iter()
            .map(|_| Int64Builder::with_capacity(arr_len))
            .collect::<Vec<_>>();
        let mut step_time_to_convert_from_start = (0..steps)
            .into_iter()
            .map(|_| Int64Builder::with_capacity(arr_len))
            .collect::<Vec<_>>();
        for k in self.buckets() {
            let b = self.buckets.get(&k).unwrap();
            total.append_value(b.total);
            completed.append_value(b.completed);
            for (step_id, step) in b.steps.iter().enumerate() {
                step_total[step_id].append_value(step.count);
                step_time_to_convert[step_id].append_value(step.time_to_convert);
                step_time_to_convert_from_start[step_id]
                    .append_value(step.time_to_convert_from_start);
            }
        }

        let arrs: Vec<ArrayRef> = vec![
            Arc::new(ts),
            Arc::new(total.finish()),
            Arc::new(completed.finish()),
        ];

        let step_total = step_total
            .iter_mut()
            .map(|v| Arc::new(v.finish()) as ArrayRef)
            .collect::<Vec<_>>();

        let step_time_to_convert = step_time_to_convert
            .iter_mut()
            .map(|v| Arc::new(v.finish()) as ArrayRef)
            .collect::<Vec<_>>();

        let step_time_to_convert_from_start = step_time_to_convert_from_start
            .iter_mut()
            .map(|v| Arc::new(v.finish()) as ArrayRef)
            .collect::<Vec<_>>();

        [
            arrs,
            step_total,
            step_time_to_convert,
            step_time_to_convert_from_start,
        ]
        .concat()
    }
}

trait Buckets: Debug + Send + Sync {
    fn push(&mut self, results: Vec<FunnelResult>);
    fn buckets(&self) -> Vec<i64>;
    fn make_new(&self) -> Self;
    fn finalize(&self) -> Vec<ArrayRef>;
}

#[derive(Debug)]
pub struct FunnelTrend {
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps: Vec<StepOrder>,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    group_cols: Vec<Column>,
    groups: Vec<SortField>,
    buckets: HashMap<OwnedRow, Box<dyn Buckets>, RandomState>,
    row_converter: RowConverter,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub ts_col: Column,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
}

impl FunnelTrend {
    pub fn new(opts: Options, buckets: Box<dyn Buckets>) -> Self {
        Self {
            ts_col: opts.ts_col,
            window: opts.window,
            steps_expr: opts
                .steps
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>(),
            steps: opts
                .steps
                .iter()
                .map(|(_, order)| order.clone())
                .collect::<Vec<_>>(),
            exclude_expr: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            buckets,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }
}

impl PartitionedAggregateExpr for FunnelTrend {
    fn group_columns(&self) -> Vec<Column> {
        todo!()
    }

    fn fields(&self) -> Vec<Field> {
        todo!()
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: &HashMap<i64, ()>,
    ) -> crate::Result<()> {
        let arrs = self
            .group_cols
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
            })
            .collect::<result::Result<Vec<_>, _>>()?;

        let rows = self.row_converter.convert_columns(&arrs)?;

        let partitions = self
            .partition_col
            .evaluate(batch)?
            .into_array(batch.num_rows())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();

        for (row_id, partition) in partitions.into_iter().enumerate() {
            let partition = partition.unwrap();
            if self.skip {
                if self.skip_partition != partition {
                    self.skip = false;
                } else {
                    continue;
                }
            }
            if !partition_exist.contains_key(&partition) {
                self.skip = true;
                continue;
            }

            let buckets = self
                .buckets
                .entry(rows.row(row_id).owned())
                .or_insert_with(|| {
                    let mut buckets = self.buckets.make_new();

                    buckets
                });

            if buckets.first {
                buckets.first = false;
                buckets.last_partition = partition;
            }
        }

        sdf
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        todo!()
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        todo!()
    }
}
