use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, mpsc};
use arrow::array::{BooleanArray, Int64Array, TimestampMillisecondArray};
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
use crate::physical_plan::segmentation::{Expr, RowResult, Spans};
use crate::physical_plan::segmentation::funnel::{funnel};
use tracing_core::Level;
use crate::{StaticArray};
use std::sync::mpsc::{Sender, Receiver};
// use crate::StaticArray;

#[derive(Debug, Clone, Default)]
pub struct Step {
    pub ts: i64,
    pub row_id: usize,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct FunnelResult {
    pub ts: Vec<i64>,
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

#[derive(Debug, Clone)]
struct Exclude {
    exists: BooleanArray,
    steps: Option<Vec<usize>>,
}

#[derive(Debug, Clone)]
struct Batch<'a> {
    pub steps: Vec<BooleanArray>,
    pub exclude: Option<Vec<Exclude>>,
    pub constants: Option<Vec<StaticArray>>,
    pub ts: TimestampMillisecondArray,
    pub batch: &'a RecordBatch,
}

struct Batches<'a> {
    batches: Vec<Batch<'a>>,
    spans: Vec<usize>,
    cur_span: usize,
}

#[derive(Debug, Clone, Default)]
struct Row {
    batch_id: usize,
    row_id: usize,
}

#[derive(Debug, Clone)]
struct Span<'a> {
    id: usize,
    offset: usize,
    len: usize,
    batches: &'a [Batch<'a>],
    const_row: Option<Row>,
    steps: Vec<Step>,
    step_id: usize,
    row_id: usize,
}

impl<'a> Span<'a> {
    pub fn new(id: usize, offset: usize, len: usize, batches: &'a [Batch]) -> Self {
        let steps = batches[0].steps.len();
        let const_row = if batches[0].constants.is_some() {
            Some(Row::default())
        } else {
            None
        };

        Self {
            id,
            offset,
            len,
            batches,
            const_row,
            steps: vec![Step::default(); steps],
            step_id: 0,
            row_id: 0,
        }
    }

    #[inline]
    pub fn abs_row_id(&self) -> (usize, usize) {
        let mut batch_id = 0;
        let mut idx = self.row_id + self.offset;
        for batch in self.batches {
            if idx < batch.batch.num_rows() {
                break;
            }
            idx -= batch.batch.num_rows();
            batch_id += 1;
        }
        (batch_id, idx)
    }

    #[inline]
    pub fn ts_value(&self) -> i64 {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].ts.value(idx)
    }

    #[inline]
    pub fn is_first_step(&self) -> bool {
        self.step_id == 0
    }

    #[inline]
    pub fn is_last_step(&self) -> bool {
        self.step_id == self.steps.len() - 1
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.is_last_step()
    }

    #[inline]
    pub fn time_window(&self) -> i64 {
        self.first_step().ts - self.cur_step().ts
    }

    #[inline]
    pub fn first_step(&self) -> &Step {
        &self.steps[0]
    }

    #[inline]
    pub fn cur_step(&self) -> &Step {
        &self.steps[self.step_id]
    }

    #[inline]
    pub fn cur_step_mut(&mut self) -> &mut Step {
        &mut self.steps[self.step_id]
    }

    #[inline]
    pub fn prev_step(&mut self) -> &mut Step {
        &mut self.steps[self.step_id - 1]
    }

    #[inline]
    pub fn next_row(&mut self) -> bool {
        println!("row_id {} len {}", self.row_id, self.len);
        if self.row_id == self.len-1 {
            return false;
        }
        self.row_id += 1;

        true
    }

    #[inline]
    pub fn next_step(&mut self) -> bool {
        println!("ttt {}", self.ts_value());
        self.steps[self.step_id].ts = self.ts_value();
        self.steps[self.step_id].row_id = self.row_id;
        if self.step_id == self.steps.len() - 1 {
            return false;
        }
        if !self.next_row() {
            println!("no more rows next_step {}", self.step_id);
            return false;
        }
        self.step_id += 1;

        true
    }


    pub fn continue_from_first_step(&mut self) -> bool {
        self.step_id = 0;
        self.next_row()
    }

    #[inline]
    pub fn continue_from_last_step(&mut self) -> bool {
        self.steps[0] = self.cur_step().clone();
        self.step_id = 0;
        self.row_id = self.cur_step().row_id;
        self.next_row()
    }

    // check if value by row id is true, so current row is a step
    // this is usually called before calling next_step
    #[inline]
    pub fn validate_cur_step(&self) -> bool {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].steps[self.step_id].value(idx)
    }

    #[inline]
    pub fn validate_constants(&mut self) -> bool {
        let (batch_id, row_id) = self.abs_row_id();
        if self.const_row.is_some() {
            if self.is_first_step() {
                self.const_row = Some(Row { row_id, batch_id });
            } else {
                let const_row = self.const_row.as_ref().unwrap();
                for (const_idx, first_const) in self.batches[const_row.batch_id].constants.as_ref().unwrap().iter().enumerate() {
                    let cur_const = &self.batches[batch_id].constants.as_ref().unwrap()[const_idx];
                    if !first_const.eq_values(const_row.row_id, cur_const, row_id) {
                        return false;
                    }
                }
            }
        }

        true
    }

    pub fn validate_excludes(&self) -> bool {
        if let Some(exclude) = &self.batches[0].exclude {
            let (batch_id, row_id) = self.abs_row_id();
            for excl in exclude.iter() {
                if excl.exists.value(row_id) {
                    return false;
                }
            }
        }

        true
    }
}

impl<'a> Batches<'a> {
    pub fn new(batches: Vec<Batch<'a>>, spans: Vec<usize>) -> Self {
        Batches {
            batches,
            spans,
            cur_span: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.batches.iter().map(|b| b.len()).sum()
    }
    pub fn next_span(&mut self) -> Option<Span> {
        if self.cur_span == self.spans.len() {
            println!("no more spans 1");
            return None;
        }

        let span_len = self.spans[self.cur_span];
        let offset = (0..self.cur_span).into_iter().map(|i| self.spans[i]).sum();
        if offset + span_len > self.len() {
            println!(" offset {offset}, span len: {span_len} > rows count: {}", self.len());
            return None;
        }
        self.cur_span += 1;
        Some(Span::new(self.cur_span - 1, offset, span_len, &self.batches))
    }
}

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
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
    dbg: Vec<DebugInfo>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum LoopResult {
    OutOfWindow,
    ConstantViolation,
    ExcludeViolation,
    NextStep,
    // PrevStep,
    NextRow,
}

pub struct Options {
    ts_col: Column,
    window: Duration,
    steps: Vec<PhysicalExprRef>,
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


#[derive(Debug, Clone, Eq, PartialEq)]
struct DebugInfo {
    loop_result: LoopResult,
    cur_span: usize,
    step_id: usize,
    row_id: usize,
    ts: i64,
}

impl Funnel {
    pub fn new(opts: Options) -> Self {
        Self {
            ts_col: opts.ts_col,
            window: opts.window,
            steps_expr: opts.steps,
            in_any_order: opts.any_order,
            exclude_expr: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            dbg: vec![],
        }
    }

    pub fn evaluate_batch<'a>(&mut self, batch: &'a RecordBatch) -> Result<Batch<'a>> {
        let mut steps = vec![];
        for (step_id, expr) in self.steps_expr.iter().enumerate() {
            // evaluate expr to bool result
            let arr = expr.evaluate(&batch)?.into_array(0).as_any().downcast_ref::<BooleanArray>().unwrap().clone();
            // add steps to state
            steps.push(arr);
        }

        // evaluate exclude
        let mut exclude = Some(vec![]);
        if let Some(exprs) = &self.exclude_expr {
            for expr in exprs.iter() {
                let arr = expr.expr.evaluate(&batch)?.into_array(0).as_any().downcast_ref::<BooleanArray>().unwrap().clone();
                if let Some(exclude) = &mut exclude {
                    let excl = Exclude { exists: arr, steps: expr.steps.clone() };
                    exclude.push(excl);
                } else {
                    let excl = Exclude { exists: arr, steps: expr.steps.clone() };
                    exclude = Some(vec![excl]);
                }
            }
        }

        // prepare constants
        let mut constants: Option<Vec<_>> = None;
        if let Some(cc) = &self.constants {
            for c in cc.iter() {
                let arr = c.evaluate(&batch)?.into_array(0);
                let arr = StaticArray::from(arr);
                if let Some(c) = &mut constants {
                    c.push(arr)
                } else {
                    constants = Some(vec![arr])
                }
            }
        }
        // timestamp column
        // Optiprism uses millisecond precision
        let ts = self
            .ts_col
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap().clone();

        let res = Batch {
            steps,
            exclude,
            constants,
            ts,
            batch,
        };

        Ok(res)
    }

    pub fn evaluate(&mut self, batches: &mut Batches) -> Result<Option<Vec<FunnelResult>>> {
        let mut results = vec![];
        // iterate over spans. For simplicity all ids are tied to span and start at 0
        'span: while let Some(mut span) = batches.next_span() {
            // destructuring for to quick access to the fields

            // main loop Until all steps are completed or we run out of rows
            // Main algorithm is:
            // 1. Find the first step to process
            // 2. optionally fix the constants
            // 3.a loop over rows until we find a row that matches the next step
            // 3.b check out of window
            // 4 break if we run out of rows or steps
            // 5. if we found a row that matches the next step, update the state
            loop {
                // default result is next row. If we find a match, we will update it
                let mut next = LoopResult::NextRow;
                // if step is not 0 and we have a window between steps and we are out of window - skip to the next funnel
                if !span.is_first_step() && span.time_window() > self.window.num_milliseconds() {
                    next = LoopResult::OutOfWindow;
                } else if span.validate_cur_step() { // if current
                    next = LoopResult::NextStep; // next step
                    if !span.validate_constants() {
                        next = LoopResult::ConstantViolation;
                    }
                }

                // check exclude between steps
                if !span.validate_excludes() {
                    next = LoopResult::ExcludeViolation;
                }

                println!("{next:?} cur span: {}, row_id: {}, step_id: {} ts: {}", span.id, span.row_id, span.step_id, span.ts_value());
                let dbinfo = DebugInfo {
                    loop_result: next.clone(),
                    cur_span: span.id,
                    step_id: span.step_id,
                    row_id: span.row_id,
                    ts: span.ts_value(),
                };
                self.dbg.push(dbinfo);

                // match result
                match next {
                    LoopResult::ExcludeViolation => {
                        span.continue_from_last_step();
                    }
                    LoopResult::NextRow => {
                        if !span.next_row() {
                            println!("no more rows");
                            break;
                        }
                    }
                    // continue funnel is usually out of window
                    LoopResult::OutOfWindow | LoopResult::ConstantViolation => {
                        if !span.continue_from_first_step() {
                            println!("no more steps");
                            break;
                        }
                    }
                    // increase step with checking
                    LoopResult::NextStep => {
                        if !span.next_step() {
                            println!("no more steps");
                            break;
                        }
                    }
                }
            }

            // final step of success decision - check filters
            let is_completed = match &self.filter {
                // if no filter, then funnel is completed id all steps are completed
                None => span.is_completed(),
                Some(filter) => match filter {
                    Filter::DropOffOnAnyStep => span.step_id != span.steps.len(),
                    // drop off on defined step
                    Filter::DropOffOnStep(drop_off_step_id) => {
                        span.step_id == *drop_off_step_id
                    }
                    // drop off if time to convert is out of range
                    Filter::TimeToConvert(from, to) => {
                        if !span.is_completed() {
                            false
                        } else {
                            span.first_step().ts >= from.num_milliseconds() && span.cur_step().ts <= to.num_milliseconds()
                        }
                    }
                }
            };

            if is_completed {
                let res = FunnelResult {
                    ts: span.steps.iter().map(|s| s.ts).collect(),
                    is_completed,
                };
                println!("{res:?}");

                results.push(res);
            }
        }

        if results.len() > 0 {
            Ok(Some(results))
        } else {
            Ok(None)
        }
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
    use tracing::log::Level::Debug;
    use tracing_core::Level;
    use tracing_test::traced_test;
    use store::test_util::parse_markdown_table;
    use crate::physical_plan::segmentation::funnel::funnel::{Batch, Batches, Count, DebugInfo, ExcludeExpr, Filter, Funnel, FunnelResult, LoopResult, Options, Step, StepExpr, Touch};
    use crate::physical_plan::segmentation::funnel::funnel::prepare_array::get_sample_events;
    use crate::error::Result;

    fn event_eq(event: &str, schema: &Schema) -> PhysicalExprRef {
        let l = Column::new_with_schema("event", schema).unwrap();
        let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
        let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
        Arc::new(expr) as PhysicalExprRef
    }

    fn evaluate_funnel(opts: Options, batches: Vec<RecordBatch>, spans: Vec<usize>, exp: Vec<DebugInfo>) -> Result<Option<Vec<FunnelResult>>> {
        let mut funnel = Funnel::new(opts);

        let mut fbatches = vec![];
        for batch in batches.iter() {
            fbatches.push(funnel.evaluate_batch(batch)?);
        }
        let mut batches = Batches::new(fbatches, spans);

        let res = funnel.evaluate(&mut batches)?;
        let i = 0;
        let exp_len = exp.len();
        assert_eq!(exp_len, funnel.dbg.len());
        for (idx, info) in exp.into_iter().enumerate() {
            assert_eq!(funnel.dbg[idx], info);
        }
        Ok(res)
    }

    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        spans: Vec<usize>,
        exp: Option<Vec<FunnelResult>>,
        exp_debug: Option<Vec<DebugInfo>>,
    }

    #[traced_test]
    #[test]
    fn test_cases() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("const", DataType::Int64, false),
        ])) as SchemaRef;

        let cases = vec![
            TestCase {
                name: "3 steps in a row".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq("e1", schema.as_ref()),
                        event_eq("e2", schema.as_ref()),
                        event_eq("e3", schema.as_ref()),
                    ],
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: Some(vec![
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 0,
                        ts: 0,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 1,
                        row_id: 1,
                        ts: 1,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 2,
                        ts: 2,
                    },
                ]),
                exp: Some(vec![FunnelResult { ts: vec![0, 1, 2], is_completed: true }]),
                spans: vec![3],
            },
            TestCase {
                name: "3 steps in a row, but span is only 2".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq("e1", schema.as_ref()),
                        event_eq("e2", schema.as_ref()),
                        event_eq("e3", schema.as_ref()),
                    ],
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: Some(vec![
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 0,
                        ts: 0,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 1,
                        row_id: 1,
                        ts: 1,
                    },
                ]),
                exp: None,
                spans: vec![2],
            },
            TestCase {
                name: "three steps in a row, starting from second row".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e2    | 1      |
| 1   | e1    | 1      |
| 2   | e2    | 1      |
| 3   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq("e1", schema.as_ref()),
                        event_eq("e2", schema.as_ref()),
                        event_eq("e3", schema.as_ref()),
                    ],
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![4],
                exp_debug: Some(vec![
                    DebugInfo {
                        loop_result: LoopResult::NextRow,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 0,
                        ts: 0,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 1,
                        ts: 1,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 1,
                        row_id: 2,
                        ts: 2,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 3,
                        ts: 3,
                    },
                ]),
                exp: Some(vec![FunnelResult { ts: vec![1, 2, 3], is_completed: true }]),
            },
            TestCase {
                name: "steps 1-3 with step 1 between".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e1    | 1      |
| 3   | e2    | 1      |
| 4   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq("e1", schema.as_ref()),
                        event_eq("e2", schema.as_ref()),
                        event_eq("e3", schema.as_ref()),
                    ],
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![5],
                exp_debug: Some(vec![
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 0,
                        ts: 0,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 1,
                        row_id: 1,
                        ts: 1,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextRow,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 2,
                        ts: 2,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextRow,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 3,
                        ts: 3,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 4,
                        ts: 4,
                    },
                ]),
                exp: Some(vec![FunnelResult { ts: vec![0, 1, 4], is_completed: true }]),
            },
            TestCase {
                name: "steps 1-3 with step exclude between".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e1    | 1      |
| 3   | e2    | 1      |
| 4   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq("e1", schema.as_ref()),
                        event_eq("e2", schema.as_ref()),
                        event_eq("e3", schema.as_ref()),
                    ],
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![5],
                exp_debug: Some(vec![
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 0,
                        row_id: 0,
                        ts: 0,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 1,
                        row_id: 1,
                        ts: 1,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextRow,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 2,
                        ts: 2,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextRow,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 3,
                        ts: 3,
                    },
                    DebugInfo {
                        loop_result: LoopResult::NextStep,
                        cur_span: 0,
                        step_id: 2,
                        row_id: 4,
                        ts: 4,
                    },
                ]),
                exp: Some(vec![FunnelResult { ts: vec![0, 1, 4], is_completed: true }]),
            },
        ];


        for case in cases {
            println!("\ntest case: {}", case.name);
            println!("============================================================");
            let (cols, schema) = get_sample_events(case.data);
            let res = evaluate_funnel(
                case.opts,
                vec![RecordBatch::try_new(schema, cols)?],
                case.spans,
                case.exp_debug.unwrap(),
            )?;
            assert_eq!(res, case.exp);
        }
        Ok(())
    }
}