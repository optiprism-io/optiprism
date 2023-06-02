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

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct Step {
    pub ts: i64,
    pub row_id: usize,
}

impl Step {
    pub fn new(ts: i64, row_id: usize) -> Self {
        Self { ts, row_id }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum FunnelResult {
    Completed(Vec<Step>),
    Incomplete(Vec<Step>, usize),
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
    steps: Option<Vec<ExcludeSteps>>,
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
    first_step: bool,
    stepn: usize,
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
            first_step: true,
            stepn: 0,
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
        self.stepn == self.steps.len() - 1
    }

    #[inline]
    pub fn time_window(&self) -> i64 {
        self.ts_value() - self.first_step().ts
        // self.first_step().ts - self.cur_step().ts-self.first_step()
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
        if self.row_id == self.len - 1 {
            return false;
        }
        self.row_id += 1;

        true
    }

    #[inline]
    pub fn next_step(&mut self) -> bool {
        if self.first_step {
            self.first_step = false;
        } else {
            self.stepn += 1;
        }
        self.steps[self.step_id].ts = self.ts_value();
        self.steps[self.step_id].row_id = self.row_id;
        if self.step_id == self.steps.len() - 1 {
            return false;
        }

        if !self.next_row() {
            return false;
        }

        self.step_id += 1;

        true
    }


    pub fn continue_from_first_step(&mut self) -> bool {
        self.step_id = 0;
        self.first_step = true;
        self.stepn = 0;
        self.row_id = self.cur_step().row_id;
        self.next_row()
    }

    #[inline]
    pub fn continue_from_last_step(&mut self) -> bool {
        if !self.next_row() {
            return false;
        }
        self.step_id = 0;
        self.first_step = true;
        self.stepn = 0;

        true
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
        let (batch_id, row_id) = self.abs_row_id();
        if let Some(exclude) = &self.batches[batch_id].exclude {
            for excl in exclude.iter() {
                let mut to_check = false;
                if let Some(steps) = &excl.steps {
                    for pair in steps {
                        if pair.from <= self.step_id && pair.to >= self.step_id {
                            to_check = true;
                        }
                    }
                } else {
                    to_check = true;
                }

                if to_check {
                    if excl.exists.value(row_id) {
                        return false;
                    }
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
            return None;
        }

        let span_len = self.spans[self.cur_span];
        let offset = (0..self.cur_span).into_iter().map(|i| self.spans[i]).sum();
        if offset + span_len > self.len() {
            (" offset {offset}, span len: {span_len} > rows count: {}", self.len());
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


#[derive(Clone, Debug)]
pub struct ExcludeSteps {
    from: usize,
    to: usize,
}

impl ExcludeSteps {
    pub fn new(from: usize, to: usize) -> Self {
        Self {
            from,
            to,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExcludeExpr {
    expr: PhysicalExprRef,
    steps: Option<Vec<ExcludeSteps>>,
}

#[derive(Clone)]
pub struct StepExpr {
    expr: PhysicalExprRef,
}

#[derive(Clone, Debug)]
enum Touch {
    First,
    Last,
    Step(usize),
}

#[derive(Clone, Debug)]
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

enum Order {
    Any,
    Asc,
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

#[derive(Debug, Clone)]
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

                // check exclude between steps
                if !span.is_first_step() && !span.validate_excludes() {
                    next = LoopResult::ExcludeViolation;
                } else if !span.is_first_step() && span.time_window() > self.window.num_milliseconds() {
                    // if step is not 0 and we have a window between steps and we are out of window - skip to the next funnel
                    next = LoopResult::OutOfWindow;
                } else if span.validate_cur_step() { // if current
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
                self.dbg.push(dbinfo);

                // match result
                match next {
                    LoopResult::ExcludeViolation => {
                        span.continue_from_last_step();
                        // span.next_row();
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
            let is_completed = match &self.filter {
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
                }
            };

            let fr = match is_completed {
                true => FunnelResult::Completed(span.steps.clone()),
                false => FunnelResult::Incomplete(span.steps[0..=span.stepn].to_vec(), span.stepn),
            };

            results.push(fr);
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
    use std::cmp::{max, min};
    use std::sync::Arc;
    use anyhow::bail;
    use arrow::array::{Array, ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
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
    use super::LoopResult::*;
    use tracing_core::Level;
    use tracing_test::traced_test;
    use store::test_util::parse_markdown_table;
    use crate::physical_plan::segmentation::funnel::funnel::{Batch, Batches, Count, DebugInfo, ExcludeExpr, ExcludeSteps, Filter, Funnel, FunnelResult, LoopResult, Options, Step, StepExpr, Touch};
    use crate::physical_plan::segmentation::funnel::funnel::prepare_array::get_sample_events;
    use crate::error::Result;

    fn event_eq(event: &str, schema: &Schema) -> PhysicalExprRef {
        let l = Column::new_with_schema("event", schema).unwrap();
        let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
        let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
        Arc::new(expr) as PhysicalExprRef
    }

    fn evaluate_funnel(opts: Options, batch: RecordBatch, spans: Vec<usize>, exp: Vec<DebugInfo>, full_debug: bool, split_by: usize) -> Result<Option<Vec<FunnelResult>>> {
        let mut funnel = Funnel::new(opts);


        let step = batch.num_rows() / split_by;
        let batches = (0..batch.num_rows()).into_iter().step_by(step).map(|o| {
            let mut n = step;
            if o + n > batch.num_rows() {
                n = batch.num_rows() - o;
            }
            let cols = batch.columns().iter().map(|col| col.slice(o, n).clone()).collect::<Vec<_>>();
            RecordBatch::try_new(batch.schema().clone(), cols).unwrap()
        }).collect::<Vec<_>>();


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
            if full_debug {
                assert_eq!(funnel.dbg[idx], info);
            } else {
                assert_eq!(funnel.dbg[idx].loop_result, info.loop_result);
            }
        }
        Ok(res)
    }

    #[derive(Debug, Clone)]
    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        spans: Vec<usize>,
        exp: Option<Vec<FunnelResult>>,
        exp_debug: Option<Vec<DebugInfo>>,
        full_debug: bool,
    }

    macro_rules! expected_debug {
    ($($ident:ident)+) => {
        Some(vec![
            $(DebugInfo {
                loop_result: LoopResult::$ident,
                cur_span: 0,
                step_id: 0,
                row_id: 0,
                ts: 0,
            },)+
        ])
    }
}

    macro_rules! steps {
    ($($ts:ident $row_id:ident)+) => {
        vec![
            $(Step::new($ts,$row_id),)+
        ]
    }
}

    macro_rules! result_completed {
    ($($ts:ident, $row_id:ident),+) => {
        FunnelResult::Completed(
        vec![
            $(Step::new($ts,$row_id),)+
        ]
        )
    }
}
    macro_rules! event_eq {
    ($schema:expr, $($name:literal)+) => {
        vec![
            $(event_eq($name, $schema.as_ref()),)+
        ]
    }
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
                name: "3 steps in a row should pass".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)])]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row, but span is only 2 should fail".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1)], 1)]),
                spans: vec![2],
                full_debug: false,
            },
            TestCase {
                name: "three steps in a row, starting from second row should pass".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![4],
                exp_debug: expected_debug!(NextRow NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(1, 1), Step::new(2, 2), Step::new(3, 3)])]),
                full_debug: false,
            },
            TestCase {
                name: "steps 1-3 with step 1 between should pass".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![5],
                exp_debug: expected_debug!(NextStep NextStep NextRow NextRow NextStep),
                // exp: Some(vec![FunnelResult { ts: vec![0, 1, 4], is_completed: true }]),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(0, 0), Step::new(1, 1), Step::new(4, 4)])]),
                full_debug: false,
            },
            TestCase {
                name: "steps 1-3 with step exclude between should pass".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | ex    | 1      |
| 3   | e3    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: Some(vec![
                        ExcludeExpr { expr: event_eq("ex", schema.as_ref()), steps: None }
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![7],
                exp_debug: expected_debug!(NextStep NextStep ExcludeViolation NextRow NextStep NextStep NextStep),
                //exp: Some(vec![FunnelResult { ts: vec![4, 5, 6], is_completed: true }]),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(4, 4), Step::new(5, 5), Step::new(6, 6)])]),

                full_debug: false,
            },
            TestCase {
                name: "steps 1-3 with step exclude e1 (1-2) should pass".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e1    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: Some(vec![
                        ExcludeExpr { expr: event_eq("e1", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(1, 2)]) }
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![6],
                exp_debug: expected_debug!(NextStep NextStep ExcludeViolation NextStep NextStep NextStep),
                //exp: Some(vec![FunnelResult { ts: vec![4, 5, 6], is_completed: true }]),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(4, 3), Step::new(5, 4), Step::new(6, 5)])]),
                full_debug: false,
            },
            TestCase {
                name: "steps 1-3 with multiple exclude should fail".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
| 7   | e1    | 1      |
| 8   | e2    | 1      |
| 9   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: Some(vec![
                        ExcludeExpr { expr: event_eq("e1", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(1, 2)]) },
                        ExcludeExpr { expr: event_eq("e2", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(0, 1)]) },
                        ExcludeExpr { expr: event_eq("e3", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(1, 2)]) },
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![9],
                exp_debug: expected_debug!(NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow),
                // exp: None,
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(7, 6)], 0)]),
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row with different const should fail".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 2      |
| 2   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: Some(vec![Column::new("const", 2)]),
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep ConstantViolation NextRow NextRow),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0)], 0)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row with same const should pass".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: Some(vec![Column::new("const", 2)]),
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                // exp: Some(vec![FunnelResult { ts: vec![0, 1, 2], is_completed: true }]),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)])]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "successful funnel with drop of on any step filter should fail".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnAnyStep),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)], 2)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "failed funnel with drop of on any step filter should fail".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e4    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnAnyStep),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1)], 1)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "successful funnel with drop of on step 1 filter should fail ".to_string(),
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
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnStep(1)),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)], 2)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "failed funnel on drop on step 1 with drop of on step 1 filter should success".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e2    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnStep(1)),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1)], 1)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "test fit in window".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 3   | e2    | 1      |
| 5   | e3    | 1      |
| 6   | e1    | 1      |
| 7   | e2    | 1      |
| 8   | e4    | 1      |
| 9   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::milliseconds(3),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep OutOfWindow NextRow NextRow NextStep NextStep NextRow NextStep),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(6, 3), Step::new(7, 4), Step::new(9, 6)])]),
                spans: vec![7],
                full_debug: false,
            },
            TestCase {
                name: "test fit window 2".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 3   | e2    | 1      |
| 5   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::milliseconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::TimeToConvert(Duration::milliseconds(2), Duration::milliseconds(10))),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(0, 0), Step::new(3, 1), Step::new(5, 2)])]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "test window".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::milliseconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::TimeToConvert(Duration::milliseconds(4), Duration::milliseconds(5))),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)], 2)]),
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row, two spans".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
| 3   | e1    | 1      |
| 4   | e2    | 1      |
| 5   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep NextStep NextStep NextStep),
                exp: Some(vec![
                    FunnelResult::Completed(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)]),
                    FunnelResult::Completed(vec![Step::new(3, 0), Step::new(4, 1), Step::new(5, 2)]),
                ]),
                spans: vec![3, 3],
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row, two spans, first fails".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e2    | 1      |
| 3   | e1    | 1      |
| 4   | e2    | 1      |
| 5   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow NextStep NextStep NextStep),
                exp: Some(vec![
                    FunnelResult::Incomplete(vec![Step::new(0, 0), Step::new(1, 1)], 1),
                    FunnelResult::Completed(vec![Step::new(3, 0), Step::new(4, 1), Step::new(5, 2)]),
                ]),
                spans: vec![3, 3],
                full_debug: false,
            },
            TestCase {
                name: "steps 1-3 with multiple exclude, 2 spans".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
| 7   | e1    | 1      |
| 8   | e2    | 1      |
| 9   | e3    | 1      |
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
| 4   | e1    | 1      |
| 5   | e2    | 1      |
| 6   | e3    | 1      |
"#,
                opts:
                Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: Some(vec![
                        ExcludeExpr { expr: event_eq("e1", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(1, 2)]) },
                        ExcludeExpr { expr: event_eq("e2", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(0, 1)]) },
                        ExcludeExpr { expr: event_eq("e3", schema.as_ref()), steps: Some(vec![ExcludeSteps::new(1, 2)]) },
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![18],
                exp_debug: expected_debug!(NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow),
                exp: Some(vec![
                    FunnelResult::Incomplete(vec![Step::new(4, 15)], 0),
                ]),
                full_debug: false,
            },
            TestCase {
                name: "3 steps in a row, two spans, second exceeds limits".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e2    | 1      |
| 2   | e3    | 1      |
| 3   | e1    | 1      |
| 4   | e2    | 1      |
| 5   | e3    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" "e2" "e3"),
                    any_order: false,
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: Some(vec![FunnelResult::Completed(vec![Step::new(0, 0), Step::new(1, 1), Step::new(2, 2)])]),
                spans: vec![3, 4],
                full_debug: false,
            },
        ];


        for split_by in 1..3 {
            println!("split batches by {split_by}");
            for case in cases.iter().cloned() {
                println!("\ntest case : {}", case.name);
                println!("============================================================");
                let (cols, schema) = get_sample_events(case.data);
                let res = evaluate_funnel(
                    case.opts,
                    RecordBatch::try_new(schema, cols)?,
                    case.spans,
                    case.exp_debug.unwrap(),
                    case.full_debug,
                    split_by,
                )?;
                assert_eq!(res, case.exp);
                println!("PASSED");
            }
        }

        Ok(())
    }
}