use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use arrow2::array::{Array, ArrayAccessor, ArrayRef, BooleanArray, Int64Array, Time, TimestampMillisecondArray, TimestampSecondArray};
use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::{Field, SchemaRef};
use arrow2::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalExprRef};
use futures::SinkExt;
use tracing::{info, log, instrument};
use tracing::log::{debug, trace};
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::segmentation::{Expr, RowResult, Spans};
use crate::physical_plan::segmentation::funnel::{funnel};
use crate::physical_plan::segmentation::funnel::per_partition::FunnelResult;
use tracing_core::Level;
use store::arrow_conversion::{arrow1_to_arrow2, arrow2_to_arrow1};
use crate::static_array::StaticArray;
use crate::{static_array, StaticArray};
// use crate::StaticArray;

#[derive(Debug, Clone)]
pub struct Step {
    pub ts: i64,
    pub row_id: usize,
    pub exists: Vec<BooleanArray>,
    pub is_completed: bool,
}

pub struct FunnelResult {
    pub steps: Vec<Step>,
    pub last_step: usize,
    pub is_completed: bool,
}

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

struct Exclude {
    exists: BooleanArray,
    steps: Option<Vec<usize>>,
}

#[derive(Debug, Clone, Default)]
struct Batch {
    pub steps: Vec<BooleanArray>,
    pub exclude: Option<Vec<Exclude>>,
    pub constants: Option<Vec<StaticArray>>,
    pub ts: Int64Array,
}

impl Batch {
    pub fn append(&mut self, other: &Batch) -> Result<()> {
        let mut steps = vec![];
        for (lstep, rstep) in self.steps.iter().zip(other.steps.iter()) {
            let res = concatenate(&[lstep.boxed().as_ref(), rstep.boxed().as_ref()])?.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            steps.push(res);
        }
        self.steps = steps;

        if let Some(e) = &self.exclude {
            let mut exclude = vec![];
            for (l, r) in e.iter().zip(other.exclude.unwrap().iter()) {
                let res = concatenate(&[l.exists.boxed().as_ref(), r.exists.boxed().as_ref()])?.as_any().downcast_ref::<BooleanArray>().unwrap().clone();

                let res = Exclude { exists: res, steps: l.steps.clone() };
                exclude.push(res);
            }
            self.exclude = Some(exclude);
        }

        if let Some(c) = &self.constants {
            let mut constants = vec![];
            for (l, r) in c.iter().zip(other.constants.unwrap().iter()) {
                let res = static_array::concatenate(l, r)?;

                constants.push(res);
            }
            self.constants = Some(constants);
        }

        let res = concatenate(&[self.ts.boxed().as_ref(), other.ts.boxed().as_ref()])?.as_any().downcast_ref::<Int64Array>().unwrap().clone();
        self.ts = res;

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct State {
    exclude: Option<Vec<Vec<BooleanArray>>>,
    batches: Vec<Batch>,
    steps: Vec<Step>,
    step_id: usize,
    row_id: usize,
}

impl State {
    pub fn new(steps: usize) -> Self {
        let step = Step {
            ts: 0,
            row_id: 0,
            exists: vec![BooleanArray::from(vec![false; 0])],
            is_completed: false,
        };
        Self {
            exclude: vec![],
            step_id: 0,
            steps: vec![step; steps],
            row_id: 0,
        }
    }
}

#[derive(Clone)]
pub struct ExcludeExpr {
    expr: PhysicalExprRef,
    steps: Option<Vec<usize>>,
}

#[derive(Clone)]
pub struct StepExpr {
    expr: PhysicalExprRef,
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
    steps_expr: Vec<PhysicalExprRef>,
    in_any_order: bool,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    state: State,
    batche: Batch,
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
    steps: Vec<StepExpr>,
    any_order: bool,
    exclude: Option<Vec<ExcludeExpr>>,
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
            steps_expr: opts.steps.clone(),
            in_any_order: opts.any_order,
            exclude_expr: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            state: State::new(opts.steps.len()),
        }
    }

    fn is_out_of_window(&self, steps: &[per_partition::Step], ts: &TimestampMillisecondArray, row_id: usize) -> bool {
        ts.value(row_id) - steps[0].ts > self.window.num_milliseconds()
    }


    pub fn pop_front_batch(&mut self) -> Option<Batch> {
        self.batches.pop_front()
    }

    pub fn evaluate_batch(&mut self, batch: &RecordBatch, schema: SchemaRef) -> Result<()> {
        // prepare steps
        let mut current_batch = Batch::default();
        for (step_id, expr) in self.steps_expr.iter().enumerate() {
            // evaluate expr to bool result
            let arr = expr.evaluate(batch)?.into_array(0);
            // convert arrow 1 to arrow 2. Because arrow 2 is faster
            let arr = arrow1_to_arrow2::convert(arr)?.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            // add steps to state
            current_batch.steps.push(arr);
        }

        // evaluate exclude
        if let Some(exprs) = &self.exclude_expr {
            for expr in exprs.iter() {
                let arr = expr.expr.evaluate(batch)?.into_array(0).as_any().downcast_ref::<BooleanArray>().unwrap().clone();
                if let Some(ex) = &mut current_batch.exclude {
                    ex.push((arr, expr.steps.clone()))
                } else {
                    current_batch.exclude = Some(vec![(arr, expr.steps.clone())])
                }
            }
        }

        // prepare constants
        if let Some(constants) = &self.constants {
            for constant in constants.iter() {
                let arr = constant.evaluate(batch)?.into_array(0);
                let arr = StaticArray::from(arr);
                if let Some(c) = &mut current_batch.constants {
                    c.push(arr)
                } else {
                    current_batch.constants = Some(vec![arr])
                }
            }
        }
        // timestamp column
        // Optiprism uses millisecond precision
        current_batch.ts = self
            .ts_col
            .evaluate(batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap().clone();

        // add batch
        self.batches.push_back(current_batch);

        Ok(())
    }

    // evaluate single partition. Partition may be spread across multiple chunks
    pub fn evaluate_partition(&mut self, chunks: &[Chunk<Box<dyn Array>>]) -> Result<FunnelResult> {
        chunks[0].chunks()
        let batch_len = batch.num_rows();
        // main loop Until all steps are completed or we run out of rows
        // Main algorithm is:
        // 1. Find the first step to process
        // 2. optionally fix the constants
        // 3.a loop over rows until we find a row that matches the next step
        // 3.b check out of window
        // 4 break if we run out of rows or steps
        // 5. if we found a row that matches the next step, update the state
        'outer: while step_id < steps.len() & &steps[step_id].row_id < batch_len {
            // default result is next row. If we find a match, we will update it
            let mut res = LoopResult::NextRow;
            // if we are out of window, we can skip to the next funnel
            if step_id > 0 & &self.is_out_of_window(&steps, &ts_col, steps[step_id].row_id) {
                res = LoopResult::ContinueFunnel;
            } else if steps[step_id].exists.value(steps[step_id].row_id) { // if the step exists
                res = LoopResult::NextStep; // next step
                if let Some((constants, const_rows)) = &mut constants {
                    // initialize constants if it is first step
                    if step_id == 0 {
                        for (const_idx, _) in constants.iter().enumerate() {
                            // assign current row_id to each constant as initial
                            const_rows[const_idx] = steps[step_id].row_id;
                        }
                    } else {
                        // for step >0 iterate over constants and check value equality
                        for (const_idx, constant) in constants.iter().enumerate() {
                            // check if current value equals to initial value
                            if !constant.eq_values(const_rows[const_idx], steps[step_id].row_id) {
                                res = LoopResult::NextRow;
                            }
                        }
                    }
                }
            }

            // check exclude between steps
            if let Some(exclude) = &exclude {
                for excl in exclude.iter() {
                    if excl.value(steps[step_id].row_id) {
                        res = LoopResult::NextFunnel;
                    }
                }
            }

            // match result
            match res {
                // continue funnel is usually out of window
                LoopResult::ContinueFunnel => {
                    step_id = 0;
                    // go back to step 0 and pick next row id
                    steps[step_id].row_id += 1;
                }
                // increase step with checking
                LoopResult::NextStep => {
                    step_id += 1;
                    if step_id > steps.len() - 1 {
                        break;
                    }
                    // assign row id of step as row id of previous step + 1
                    steps[step_id].row_id = steps[step_id - 1].row_id + 1;
                }
                // just go no the next row
                LoopResult::NextRow => {
                    steps[step_id].row_id += 1;
                }
                // next funnel
                LoopResult::NextFunnel => {
                    // greedy variant. Treat next row of first step as a new funnel
                    steps[0].row_id = steps[step_id].row_id + 1;
                    step_id = 0;
                }
            }
            // double check limits
            if step_id > = steps.len() || steps[step_id].row_id > = batch_len {
                break;
            }

            // assign timestamp on each iteration
            steps[step_id].ts = ts_col.value(steps[step_id].row_id);
        }

        // final step of success decision - check filters
        let is_completed = match &self.filter {
            // if no filter, then funnel is completed id all steps are completed
            None => step_id == steps.len() - 1,
            Some(filter) => match filter {
                Filter::DropOffOnAnyStep => step_id != steps.len(),
                // drop off on defined step
                Filter::DropOffOnStep(drop_off_step_id) => {
                    step_id == *drop_off_step_id
                }
                // drop off if time to convert is out of range
                Filter::TimeToConvert(from, to) => {
                    if step_id != steps.len() - 1 {
                        false
                    } else {
                        steps[0].ts > = from.num_milliseconds() & &steps[step_id].ts < = to.num_milliseconds()
                    }
                }
            }
        };
    };

    let res = FunnelResult {
    steps,
    last_step: step_id,
    is_completed,
};
Ok(res)
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

    pub fn get_sample_events(data: &str) -> (Vec<ArrayRef>, SchemaRef) {
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
    use crate::physical_plan::segmentation::funnel::funnel::{Count, ExcludeExpr, Filter, Funnel, Options, Step, StepExpr, Touch};
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
        let (cols, schema) = get_sample_events(data);

        let opts = Options {
            schema: schema.clone(),
            ts_col: Column::new("ts", 0),
            window: Duration::seconds(100),
            steps: vec![
                StepExpr {
                    expr: event_eq("e1", schema.as_ref()),
                    comparison: None,
                },
                StepExpr {
                    expr: event_eq("e2", schema.as_ref()),
                    comparison: None,
                },
                StepExpr {
                    expr: event_eq("e3", schema.as_ref()),
                    comparison: None,
                },
            ],
            any_order: false,
            exclude: Some(vec![ExcludeExpr {
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
        funnel.evaluate_partition(vec![1, 2, 3].as_slice(), &batch, true)?;

        Ok(())
    }
}