use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::BooleanBuilder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::TimestampMillisecondArray;
use arrow::array::TimestampMillisecondBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::struct_expressions::struct_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::DisplayFormatType::Default;
use datafusion_common::ScalarValue;
use futures::SinkExt;
use tracing::info;
use tracing::instrument;
use tracing::log;
use tracing::log::debug;
use tracing::log::trace;
use tracing_core::Level;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::abs_row_id_refs;
use crate::physical_plan::expressions::partitioned::funnel::evaluate_batch;
use crate::physical_plan::expressions::partitioned::funnel::next_span;
use crate::physical_plan::expressions::partitioned::funnel::Batch;
use crate::physical_plan::expressions::partitioned::funnel::Count;
use crate::physical_plan::expressions::partitioned::funnel::Count::Unique;
use crate::physical_plan::expressions::partitioned::funnel::ExcludeExpr;
use crate::physical_plan::expressions::partitioned::funnel::Filter;
use crate::physical_plan::expressions::partitioned::funnel::FunnelResult;
use crate::physical_plan::expressions::partitioned::funnel::Span;
use crate::physical_plan::expressions::partitioned::funnel::StepOrder;
use crate::physical_plan::expressions::partitioned::funnel::Touch;
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
use crate::StaticArray;

#[derive(Debug)]
struct BucketStep {
    time_to_convert: Int64Builder,
    time_to_convert_from_start: Int64Builder,
}

impl BucketStep {
    pub fn new() -> Self {
        Self {
            time_to_convert: Int64Builder::with_capacity(1000),
            time_to_convert_from_start: Int64Builder::with_capacity(1000),
        }
    }
}

#[derive(Debug)]
struct Bucket {
    ts: i64,
    is_completed: BooleanBuilder,
    completed_steps: Int64Builder,
    steps: Vec<BucketStep>,
}

impl Bucket {
    pub fn new(ts: i64, steps: usize) -> Self {
        let s = (0..steps).into_iter().map(|_| BucketStep::new()).collect();
        Self {
            ts,
            is_completed: BooleanBuilder::with_capacity(1000),
            completed_steps: Int64Builder::with_capacity(1000),
            steps: s,
        }
    }
}

#[derive(Debug)]
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
            buckets.insert(k, Bucket::new(k, steps_count));
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
                    None
                } else {
                    Some(step.ts - result_steps[idx - 1].ts)
                };
                let time_to_convert_from_start = if idx == 0 {
                    None
                } else {
                    Some(step.ts - result_steps[0].ts)
                };
                let bucket = self.buckets.get_mut(&k).unwrap();
                bucket.completed_steps.append_value(completed_steps as i64);
                bucket.is_completed.append_value(is_completed);

                for step in 0..self.steps_count {
                    if step > completed_steps {
                        bucket.steps[step].time_to_convert.append_null();
                        bucket.steps[step].time_to_convert_from_start.append_null();
                    } else {
                        bucket.steps[step]
                            .time_to_convert
                            .append_option(time_to_convert);
                        bucket.steps[step]
                            .time_to_convert_from_start
                            .append_option(time_to_convert_from_start);
                    }
                }
            }
        }
    }

    fn buckets(&self) -> Vec<i64> {
        self.ts.clone()
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        let mut res = Vec::with_capacity(2 + self.steps_count * 2);
        for k in self.ts.clone() {
            let bucket = self.buckets.get_mut(&k).unwrap();
            res.push(Arc::new(bucket.is_completed.finish()) as ArrayRef);
            res.push(Arc::new(bucket.completed_steps.finish()) as ArrayRef);
            for step in bucket.steps.iter_mut() {
                res.push(Arc::new(step.time_to_convert.finish()) as ArrayRef);
                res.push(Arc::new(step.time_to_convert_from_start.finish()) as ArrayRef);
            }
        }

        res
    }
}

trait Buckets: Debug + Send + Sync {
    fn push(&mut self, results: Vec<FunnelResult>);
    fn buckets(&self) -> Vec<i64>;
    fn finish(&mut self) -> Vec<ArrayRef>;
}

#[derive(Debug)]
pub struct FunnelExpr {
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
    dbg: Vec<DebugInfo>,
    buckets: Mutex<Box<dyn Buckets>>,
    name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LoopResult {
    OutOfWindow,
    ConstantViolation,
    ExcludeViolation,
    NextStep,
    // PrevStep,
    NextRow,
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
    pub name: String,
}

pub enum Error {
    OutOfWindow,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DebugInfo {
    loop_result: LoopResult,
    cur_span: usize,
    step_id: usize,
    row_id: usize,
    ts: i64,
}

impl FunnelExpr {
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
            dbg: vec![],
            buckets: Mutex::new(buckets),
            name: opts.name,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }
}

impl PartitionedAggregateExpr for FunnelExpr {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let spans_len = spans.len();
        // evaluate each batch, e.g. create batch state
        let batches = batches
            .iter()
            .map(|b| {
                evaluate_batch(
                    b,
                    &self.steps_expr,
                    &self.exclude_expr,
                    &self.constants,
                    &self.ts_col,
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut cur_span = 0;

        let steps = self.steps.clone();
        // if skip is set, we need to skip the first span
        let spans = if skip > 0 {
            let spans = [vec![skip], spans].concat();
            next_span(&batches, &spans, &mut cur_span, &steps);
            spans
        } else {
            spans
        };

        let (window, filter) = (self.window.clone(), self.filter.clone());
        let mut dbg: Vec<DebugInfo> = vec![];
        // iterate over spans. For simplicity all ids are tied to span and start at 0
        let mut funnel_results: Vec<Vec<FunnelResult>> = Vec::with_capacity(spans_len);
        let mut buckets = self.buckets.lock().unwrap();
        let mut span_results: Vec<FunnelResult> = vec![];
        while let Some(mut span) = next_span(&batches, &spans, &mut cur_span, &steps) {
            while span.is_next_row() {
                loop {
                    // default result is next row. If we find a match, we will update it
                    let mut next = LoopResult::NextRow;

                    // check exclude between steps
                    if !span.is_first_step() && !span.validate_excludes() {
                        next = LoopResult::ExcludeViolation;
                    } else if !span.is_first_step()
                        && span.time_window() > window.num_milliseconds()
                    {
                        // if step is not 0 and we have a window between steps and we are out of window - skip to the next funnel
                        next = LoopResult::OutOfWindow;
                    } else if span.validate_cur_step() {
                        // if current
                        next = LoopResult::NextStep; // next step
                        if !span.validate_constants() {
                            next = LoopResult::ConstantViolation;
                        }
                    }

                    let dbinfo = DebugInfo {
                        loop_result: next.clone(),
                        cur_span: span.id,
                        step_id: span.step_id,
                        row_id: span.row_id,
                        ts: span.ts_value(),
                    };
                    dbg.push(dbinfo);

                    // match result
                    match next {
                        LoopResult::ExcludeViolation => {
                            span.continue_from_last_step();
                        }
                        LoopResult::NextRow => {
                            if !span.next_row() {
                                break;
                            }
                        }
                        // continue funnel is usually out of window
                        LoopResult::OutOfWindow | LoopResult::ConstantViolation => {
                            if !span.continue_from_first_step() {
                                break;
                            }
                        }
                        // increase step with checking
                        LoopResult::NextStep => {
                            if !span.next_step() {
                                break;
                            }
                        }
                    }
                }

                // final step of success decision - check filters
                let is_completed = match &filter {
                    // if no filter, then funnel is completed id all steps are completed
                    None => span.is_completed(),
                    Some(filter) => match filter {
                        Filter::DropOffOnAnyStep => !span.is_last_step(),
                        // drop off on defined step
                        Filter::DropOffOnStep(drop_off_step_id) => {
                            span.step_id == *drop_off_step_id
                        }
                        // drop off if time to convert is out of range
                        Filter::TimeToConvert(from, to) => {
                            if !span.is_completed() {
                                false
                            } else {
                                let diff = span.cur_step().ts - span.first_step().ts;
                                from.num_milliseconds() <= diff && diff <= to.num_milliseconds()
                            }
                        }
                    },
                };

                let fr = match is_completed {
                    true => FunnelResult::Completed(span.steps.clone()),
                    false => {
                        FunnelResult::Incomplete(span.steps[0..=span.stepn].to_vec(), span.stepn)
                    }
                };

                span_results.push(fr);

                if span.is_next_row() {
                    span.continue_from_last_step();
                }
            }
            funnel_results.push(span_results.drain(..).collect::<Vec<_>>());
        }

        Ok(buckets.finish())
    }

    fn fields(&self) -> Vec<Field> {
        let mut fields = vec![];
        for v in self.buckets.lock().unwrap().buckets() {
            fields.push(Field::new(
                format!("{}_{},is_completed", self.name, v),
                DataType::Boolean,
                false,
            ));
            fields.push(Field::new(
                format!("{}_{}_completed_steps", self.name, v),
                DataType::Int64,
                false,
            ));
            for i in 0..self.steps.len() {
                fields.push(Field::new(
                    format!("{}_{}_step_{}_time_to_convert", self.name, v, i),
                    DataType::Int64,
                    false,
                ));
                fields.push(Field::new(
                    format!("{}_{}_step_{}_time_to_convert_from_start", self.name, v, i),
                    DataType::Int64,
                    false,
                ));
            }
        }
        fields
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.fields()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use chrono::Duration;
    use chrono::DurationRound;
    use chrono::NaiveDateTime;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::expressions::Column;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use futures::SinkExt;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::BucketStep;
    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::DateTimeBuckets;
    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::FunnelExpr;
    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::Options;
    use crate::physical_plan::expressions::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned::funnel::Touch;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;

    #[test]
    fn test_funnel() {
        let data = r#"
| user_id(i64) | ts(ts)              | event(utf8) |
|--------------|---------------------|-------------|
| 0            | 2020-04-12 22:10:57 | e1          |
| 0            | 2020-04-13 21:10:57 | e2          |
| 0            | 2020-04-14 00:00:00 | e3          |
| 0            | 2020-04-15 01:10:57 | e1          |
| 0            | 2020-04-16 02:10:00 | e2          |
| 0            | 2020-04-17 03:10:57 | e3          |
| 0            | 2020-04-18 04:10:57 | e1          |
| 0            | 2020-04-19 05:10:57 | e2          |
| 1            | 2020-04-12 22:10:57 | e1          |
| 1            | 2020-04-13 21:10:57 | e2          |
| 1            | 2020-04-14 00:00:00 | e3          |
| 1            | 2020-04-15 01:10:57 | e1          |
| 1            | 2020-04-16 02:10:00 | e2          |
| 1            | 2020-04-17 03:10:57 | e3          |
| 1            | 2020-04-18 04:10:57 | e1          |
| 1            | 2020-04-19 05:10:57 | e2          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema().clone();
        let e1 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e2".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e3".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ds = Duration::days(1);
        let start = NaiveDateTime::parse_from_str("2020-04-12 22:10:57", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .duration_trunc(ds)
            .unwrap()
            .timestamp_millis();
        let end = start + Duration::days(90).num_milliseconds();
        let v = (start..end)
            .into_iter()
            .step_by(ds.num_milliseconds() as usize)
            .collect::<Vec<_>>();
        let buckets = Box::new(DateTimeBuckets::new(3, v, ds));

        let opts = Options {
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            window: Duration::seconds(15),
            steps: vec![e1, e2, e3.clone()],
            exclude: None,
            constants: None,
            count: Unique,
            filter: None,
            touch: Touch::First,
            name: "f".to_string(),
        };
        let mut f = FunnelExpr::new(opts, buckets);

        let spans = vec![8, 8];
        let arrs = f.evaluate(&batches, spans, 0).unwrap();
        let batch = RecordBatch::try_new(f.schema(), arrs).unwrap();
        print_batches(&vec![batch]).unwrap();
    }
}
