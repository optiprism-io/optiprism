use std::collections::HashMap;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use futures::SinkExt;
use tracing::{info, log, instrument};
use tracing::log::{debug, trace};
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::{Expr, Spans};
use crate::physical_plan::segmentation::funnel::{funnel, per_partition};
use crate::physical_plan::segmentation::funnel::per_partition::FunnelResult;
use tracing_core::Level;

enum Report {
    Steps,
    Trends,
    TimeToConvert,
    Frequency,
    TopPaths,
}


#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

#[derive(Debug, Clone)]
struct State {
    step_id: usize,
    // row_id: usize,
    step_row: Vec<usize>,
    window_start_ts: i64,
    is_completed: bool,
    // result: FunnelResult,
}

impl State {
    pub fn new(steps: usize) -> Self {
        Self {
            step_id: 0,
            // row_id: 0,
            step_row: vec![0; steps],
            window_start_ts: 0,
            is_completed: false,
        }
    }
}

#[derive(Clone)]
pub struct Exclude {
    expr: PhysicalExprRef,
    steps: Option<Vec<usize>>,
}

#[derive(Clone)]
pub struct Step {
    expr: PhysicalExprRef,
    comparison: Option<Vec<Box<Step>>>,
}

enum Touch {
    First,
    Last,
    Step(usize),
}

enum Count {
    Unique,
    NonUnique,
    Session,
}

pub struct Funnel {
    schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps: Vec<Step>,
    in_any_order: bool,
    exclude: Option<Vec<Exclude>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    state: State,
    spans: Spans,

}

#[derive(Debug, Clone)]
enum LoopResult {
    ContinueFunnel,
    NextFunnel,
    NextStep,
    // PrevStep,
    NextRow,
}

pub struct Options {
    schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps: Vec<Step>,
    any_order: bool,
    exclude: Option<Vec<Exclude>>,
    constants: Option<Vec<Column>>,
    count: Count,
    filter: Option<Filter>,
    touch: Touch,
}

enum Error {
    OutOfWindow
}

impl Funnel {
    pub fn new(opts: Options) -> Self {
        Self {
            schema: opts.schema,
            ts_col: opts.ts_col,
            window: opts.window,
            steps: opts.steps.clone(),
            in_any_order: opts.any_order,
            exclude: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            state: State::new(opts.steps.len()),
            spans: Spans::new(100),
        }
    }

    fn is_out_of_window(&self, steps: &[per_partition::Step], ts: &TimestampMillisecondArray, row_id: usize) -> bool {
        ts.value(row_id) - steps[0].ts > self.window.num_milliseconds()
    }


    pub fn evaluate(&mut self, spans: &[usize], batch: &RecordBatch, is_last: bool) -> Result<bool> {
        let batch_len = batch.columns()[0].len();
        let mut steps = self
            .steps
            .iter()
            .map(|step| {
                step.expr
                    .evaluate(batch)
                    .map(|v| {
                        let arr = v.
                            into_array(0).
                            as_any().
                            downcast_ref::<BooleanArray>().
                            unwrap().
                            clone();
                        per_partition::Step {
                            ts: 0,
                            row_id: 0,
                            exists: arr,
                            is_completed: false,
                        }
                    }
                    )
                    .map_err(|e| e.into())
            })
            .collect::<Result<Vec<_>>>()?;


        let exclude = self.exclude.clone().map(|e| {
            e.
                iter().
                map(|f| f.expr
                    .evaluate(batch)
                    .map(|d| d.
                        into_array(0).
                        as_any().
                        downcast_ref::<BooleanArray>().
                        unwrap().
                        clone()).
                    map_err(|e| e.into())).
                collect::<Result<Vec<_>>>().unwrap()
        });

        let mut constants = self.constants.as_ref().map(|constants| {
            let v = constants
                .iter()
                .map(|constant| {
                    constant
                        .evaluate(batch)
                        .map(|v| v.into_array(0).clone())
                        .map_err(|e| e.into())
                })
                .collect::<Result<Vec<_>>>().unwrap();
            (v, vec![0; constants.len()])
        });

        let ts_col = self
            .ts_col
            .evaluate(batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap().clone();

        let State {
            mut step_id,
            // mut row_id,
            mut step_row,
            mut window_start_ts,
            mut is_completed,
        } = self.state.clone();


        // main loop
        'outer: while step_id < steps.len() && steps[step_id].row_id < batch_len {
            let mut res = LoopResult::NextRow;
            if step_id > 0 && self.is_out_of_window(&steps, &ts_col, steps[step_id].row_id) {
                res = LoopResult::ContinueFunnel;
            } else if steps[step_id].exists.value(steps[step_id].row_id) {
                res = LoopResult::NextStep;
                if let Some((constants, const_rows)) = &mut constants {
                    if step_id == 0 {
                        for (const_idx, constant) in constants.iter().enumerate() {
                            const_rows[const_idx] = steps[step_id].row_id;
                        }
                    } else {
                        for (const_idx, constant) in constants.iter().enumerate() {
                            // todo make static
                            let constant = constant.as_any().downcast_ref::<Int64Array>().unwrap();
                            if constant.value(const_rows[const_idx]) != constant.value(steps[step_id].row_id) {
                                res = LoopResult::NextRow;
                            }
                        }
                    }
                }
            }


            if let Some(exclude) = &exclude {
                for excl in exclude.iter() {
                    if excl.value(steps[step_id].row_id) {
                        res = LoopResult::NextFunnel;
                    }
                }
            }

            println!("{:?} {} {}", res, step_id, steps[step_id].row_id);
            match res {
                LoopResult::ContinueFunnel => {
                    step_id = 0;
                    steps[step_id].row_id += 1;
                }
                LoopResult::NextStep => {
                    step_id += 1;
                    if step_id > steps.len() - 1 {
                        break;
                    }
                    steps[step_id].row_id = steps[step_id - 1].row_id + 1;
                }
                LoopResult::NextRow => {
                    steps[step_id].row_id += 1;
                }
                LoopResult::NextFunnel => {
                    steps[0].row_id = steps[step_id].row_id + 1;
                    step_id = 0;
                }
            }
            steps[step_id].ts = ts_col.value(steps[step_id].row_id);
        }

        let is_completed = match &self.filter {
            None => true,
            Some(filter) => match filter {
                Filter::DropOffOnAnyStep => step_id != steps.len(),
                Filter::DropOffOnStep(drop_off_step_id) => {
                    step_id == *drop_off_step_id
                }
                Filter::TimeToConvert(from, to) => {
                    if step_id != steps.len() {
                        false
                    } else {
                        steps[0].ts >= from.num_milliseconds() && steps[0].ts <= to.num_milliseconds()
                    }
                }
            }
        };

        info!("is_completed: {}", is_completed);
        for (idx, step) in steps.iter().enumerate() {
            println!("step {}: {:?}", idx, step);
        }
        Ok(is_completed)
    }
}

mod prepare_array {
    use std::sync::Arc;
    use arrow2::array::Utf8Array;
    use arrow2::datatypes::{DataType, Field, TimeUnit};
    use arrow::array;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{Schema, SchemaRef};
    use datafusion::physical_expr::expressions::Literal;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;

    pub fn get_sample_events() -> (Vec<ArrayRef>, SchemaRef) {
        let data = r#"
| ts | user_id | event | const |
|----|---------|-------|-------|
| 1  | 1       | e1    | 1     |
| 2  | 1       | e2    | 1     |
| 2  | 1       | e2    | 1     |
| 2  | 1       | e1    | 1     |
| 4  | 1       | e3    | 2     |
| 5  | 1       | e1    | 1     |
| 6  | 1       | e2    | 1     |
| 6  | 1       | e4    | 1     |
| 7  | 1       | e3    | 1     |
| 8  | 1       | e1    | 1     |
| 9  | 1       | e2    | 1     |
| 10 | 1       | e3    | 1     |
"#;
        let fields = vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("event", DataType::Utf8, true),
            Field::new("const", DataType::Int64, true),
        ];
        let res = parse_markdown_table(data, &fields).unwrap();

        let (arrs, fields) = res.
            into_iter().
            zip(fields).
            map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap()).
            unzip();

        let schema = Arc::new(Schema::new(fields)) as SchemaRef;

        (arrs, schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use anyhow::bail;
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
    use datafusion::physical_expr::unicode_expressions::right;
    use datafusion_common::ScalarValue;
    use datafusion_expr::{binary_expr, ColumnarValue, Expr, Operator};
    use tracing::{debug, info};
    use tracing_core::Level;
    use tracing_test::traced_test;
    use store::test_util::parse_markdown_table;
    use crate::physical_plan::segmentation::funnel::funnel::{Count, Exclude, Filter, Funnel, Options, Step, Touch};
    use crate::physical_plan::segmentation::funnel::funnel::prepare_array::get_sample_events;

    fn event_eq(event: &str, schema: &Schema) -> PhysicalExprRef {
        let l = Column::new_with_schema("event", schema).unwrap();
        let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
        let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
        Arc::new(expr) as PhysicalExprRef
    }

    #[traced_test]
    #[test]
    fn it_works() -> anyhow::Result<()> {
        let subscriber = tracing_subscriber::fmt()
            // Use a more compact, abbreviated log format
            .compact()
            // Display source code file paths
            .with_file(true)
            // Display source code line numbers
            .with_line_number(true)
            // Display the thread ID an event was recorded on
            .with_thread_ids(true)
            // Don't display the event's target (module path)
            .with_target(false)
            .with_max_level(Level::TRACE)
            // Build the subscriber
            .finish();

        let (cols, schema) = get_sample_events();

        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new("ts", 0),
            window: Duration::milliseconds(1),
            steps: vec![
                Step {
                    expr: event_eq("e1", schema.as_ref()),
                    comparison: None,
                },
                Step {
                    expr: event_eq("e2", schema.as_ref()),
                    comparison: None,
                },
                Step {
                    expr: event_eq("e3", schema.as_ref()),
                    comparison: None,
                },
            ],
            any_order: false,
            exclude: Some(vec![Exclude {
                expr: event_eq("e4", schema.as_ref()),
                steps: None,
            }]),
            constants: Some(vec![Column::new_with_schema("const", schema.as_ref())?]),
            count: Count::Unique,
            filter: Some(Filter::DropOffOnAnyStep),
            touch: Touch::First,
        };

        let mut funnel = Funnel::new(opts);


        let batch = RecordBatch::try_new(schema, cols)?;
        funnel.evaluate(vec![1, 2, 3].as_slice(), &batch, true)?;

        Ok(())
    }
}