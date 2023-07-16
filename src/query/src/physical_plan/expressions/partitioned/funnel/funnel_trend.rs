use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;
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
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::struct_expressions::struct_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
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

#[derive(Clone, Debug)]
struct DebugBuckets {}

impl Buckets for DebugBuckets {
    fn add(&mut self, result: &FunnelResult) -> Result<()> {
        println!("{:?}", result);
        Ok(())
    }
}

trait Buckets: Debug {
    fn add(&mut self, result: &FunnelResult) -> Result<()>;
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
    buckets: Box<dyn Buckets>,
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
            buckets,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }

    // entry point
    pub fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<()> {
        // evaluate each batch, e.g. create batch state
        let batches = record_batches
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

                self.buckets.add(&fr)?;

                if span.is_next_row() {
                    span.continue_from_last_step();
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Duration;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::expressions::Column;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::DebugBuckets;
    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::FunnelExpr;
    use crate::physical_plan::expressions::partitioned::funnel::funnel_trend::Options;
    use crate::physical_plan::expressions::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned::funnel::Touch;

    #[test]
    fn test_funnel() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | e1          |
| 0            | 2      | e2          |
| 0            | 3      | e3          |
| 0            | 4      | e1          |
| 0            | 5      | e2          |
| 0            | 6      | e3          |
| 0            | 7      | e1          |
| 0            | 8      | e2          |
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

        let opts = Options {
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            window: Duration::seconds(15),
            steps: vec![e1, e2, e3],
            exclude: None,
            constants: None,
            count: Unique,
            filter: None,
            touch: Touch::First,
        };

        let buckets = Box::new(DebugBuckets {});
        let mut f = FunnelExpr::new(opts, buckets);

        let spans = vec![8];
        f.evaluate(&batches, spans, 0).unwrap();
    }
}
