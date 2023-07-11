use std::collections::HashMap;
use std::collections::VecDeque;
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
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use futures::SinkExt;
use tracing::info;
use tracing::instrument;
use tracing::log;
use tracing::log::debug;
use tracing::log::trace;
use tracing_core::Level;

use super::*;
use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::abs_row_id_refs;
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
use crate::StaticArray;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StepOrder {
    Sequential,
    Any(Vec<usize>), // any of the steps
}

// step that was fixed
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Step {
    pub ts: i64,
    // timestamp at the time of fix
    pub row_id: usize,
    // row id
    pub order: StepOrder, // order of the step. Steps may be in different order
}

impl Step {
    pub fn new(ts: i64, row_id: usize, order: StepOrder) -> Self {
        Self { ts, row_id, order }
    }
    pub fn new_sequential(ts: i64, row_id: usize) -> Self {
        Self {
            ts,
            row_id,
            order: StepOrder::Sequential,
        }
    }

    pub fn new_any(ts: i64, row_id: usize, any: Vec<usize>) -> Self {
        Self {
            ts,
            row_id,
            order: StepOrder::Any(any),
        }
    }
}

// Result of the funnel
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FunnelResult {
    Completed(Vec<Step>),
    // completed result with all the steps
    Incomplete(Vec<Step>, usize),
    // incompleted result with completed steps and count of completed steps
}

// Additional filter to complete
#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,
    // funnel should fail on any step
    DropOffOnStep(usize),
    // funnel should fail on certain step
    TimeToConvert(Duration, Duration), // conversion should be within certain window
}

// exclude steps
#[derive(Debug, Clone)]
pub struct Exclude {
    exists: BooleanArray,
    // array of booleans that indicate if the step exists
    // optional array of steps to apply exclude only between them. Otherwise include
    // will be applied on each step
    steps: Option<Vec<ExcludeSteps>>,
}

// Batch for state
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub steps: Vec<BooleanArray>,
    // boolean Exists array for each step
    // pointer to step_results
    pub exclude: Option<Vec<Exclude>>,
    // optional exclude with Exists array
    pub constants: Option<Vec<StaticArray>>,
    // optional constants
    pub ts: TimestampMillisecondArray,
    // timestamp
    pub batch: &'a RecordBatch, // ref to actual record batch
}

// Relative row with batch id and row id. Used mostly for constants
#[derive(Debug, Clone, Default)]
struct Row {
    batch_id: usize,
    row_id: usize,
}

// Span is a span of rows that are in the same partition
#[derive(Debug, Clone)]
pub struct Span<'a> {
    id: usize,
    // # of span
    offset: usize,
    // offset of the span. Used to skip rows from record batch. See PartitionedState
    len: usize,
    // length of the span
    batches: &'a [Batch<'a>],
    // one or more batches with span
    // fixed const row. This will be filled with fiest occurrence if constants are enabled and
    const_row: Option<Row>,
    steps: Vec<Step>,
    // state of steps
    step_id: usize,
    // current step
    row_id: usize,
    // current row id
    first_step: bool,
    // is this first step
    stepn: usize, // supplement counter for completed steps
}

impl<'a> Span<'a> {
    pub fn new(
        id: usize,
        offset: usize,
        len: usize,
        steps: &[StepOrder],
        batches: &'a [Batch],
    ) -> Self {
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
            steps: steps
                .iter()
                .map(|step| Step::new(0, 0, step.to_owned()))
                .collect(),
            step_id: 0,
            row_id: 0,
            first_step: true,
            stepn: 0,
        }
    }

    // Calculates absolute row for span row id and offset
    // Absolute row refers to batch id and row id in the batch
    #[inline]
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

    // get ts value of current row
    #[inline]
    pub fn ts_value(&self) -> i64 {
        // calculate batch id and row id
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
    }

    #[inline]
    pub fn first_step(&self) -> &Step {
        &self.steps[0]
    }

    #[inline]
    pub fn cur_step(&self) -> &Step {
        &self.steps[self.step_id]
    }

    // go to next row
    #[inline]
    pub fn next_row(&mut self) -> bool {
        if self.row_id == self.len - 1 {
            return false;
        }
        self.row_id += 1;

        true
    }

    // go to next step
    #[inline]
    pub fn next_step(&mut self) -> bool {
        if self.first_step {
            self.first_step = false;
        } else {
            self.stepn += 1;
        }
        // 1. assign ts value and row id to current step. To not to assign it each time
        // 2. increment row
        // 3. increment step
        self.steps[self.step_id].ts = self.ts_value();
        self.steps[self.step_id].row_id = self.row_id;
        if self.step_id == self.steps.len() - 1 {
            return false;
        }

        // increment row
        if !self.next_row() {
            return false;
        }

        // increment step for
        self.step_id += 1;

        true
    }

    #[inline]
    pub fn continue_from_first_step(&mut self) -> bool {
        // reset everything
        self.step_id = 0;
        self.first_step = true;
        self.stepn = 0;
        self.row_id = self.cur_step().row_id;
        // make next row
        self.next_row()
    }

    #[inline]
    pub fn continue_from_last_step(&mut self) -> bool {
        // move caret to the next tow
        if !self.next_row() {
            return false;
        }
        // make this row as first step
        self.step_id = 0;
        self.first_step = true;
        self.stepn = 0;

        true
    }

    // check if value by row id is true, thus current row is a step
    // this is usually called before calling next_step
    //  1
    // *2
    // *3
    //  4
    // *5
    //  6
    #[inline]
    pub fn validate_cur_step(&mut self) -> bool {
        // get abs row
        let (batch_id, idx) = self.abs_row_id();
        let mut del_step = None; // todo what is this?
        // choose order strategy
        match &mut self.steps[self.step_id].order {
            StepOrder::Sequential => {
                return if del_step.is_none() {
                    // just return current Exists value
                    self.batches[batch_id].steps[self.step_id].value(idx)
                } else {
                    true
                };
            }
            StepOrder::Any(steps) => {
                if let Some(step_id) = del_step {
                    steps.retain(|&x| x != step_id);
                }
                for step_id in steps {
                    if self.batches[batch_id].steps[*step_id].value(idx) {
                        del_step = Some(*step_id);
                        break;
                    }
                }
            }
        }

        del_step.is_some()
    }

    #[inline]
    pub fn validate_constants(&mut self) -> bool {
        let (batch_id, row_id) = self.abs_row_id();
        // fix the constant if this is the first step
        if self.const_row.is_some() {
            if self.is_first_step() {
                // save constant row info
                self.const_row = Some(Row { row_id, batch_id });
                return true;
            } else {
                // compare current value with constant
                // get constant row
                let const_row = self.const_row.as_ref().unwrap();
                for (const_idx, first_const) in self.batches[const_row.batch_id]
                    .constants
                    .as_ref()
                    .unwrap()
                    .iter()
                    .enumerate()
                {
                    // compare the const values of current row and first row
                    let cur_const = &self.batches[batch_id].constants.as_ref().unwrap()[const_idx];
                    if !first_const.eq_values(const_row.row_id, cur_const, row_id) {
                        return false;
                    }
                }
            }
        }

        true
    }

    // check if there is no excludes between the steps
    pub fn validate_excludes(&self) -> bool {
        let (batch_id, row_id) = self.abs_row_id();
        if let Some(exclude) = &self.batches[batch_id].exclude {
            for excl in exclude.iter() {
                let mut to_check = false;
                // check if this exclude is relevant to current step
                if let Some(steps) = &excl.steps {
                    for pair in steps {
                        if pair.from <= self.step_id && pair.to >= self.step_id {
                            to_check = true;
                            break;
                        }
                    }
                } else {
                    // check anyway
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
        Self { from, to }
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
pub enum Touch {
    First,
    Last,
    Step(usize),
}

#[derive(Clone, Debug)]
pub enum Count {
    Unique,
    NonUnique,
    Session,
}

#[derive(Clone, Debug)]
pub struct FunnelExpr {
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps: Vec<StepOrder>,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    cur_span: usize,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    dbg: Vec<DebugInfo>,
}

pub enum Order {
    Any,
    Asc,
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
    pub fn new(opts: Options) -> Self {
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
            cur_span: 0,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            dbg: vec![],
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }
    // calculate expressions
    fn evaluate_batch<'a>(&mut self, batch: &'a RecordBatch) -> Result<Batch<'a>> {
        let mut steps = vec![];
        // evaluate steps
        for expr in self.steps_expr.iter() {
            // evaluate expr to bool result
            let arr = expr
                .evaluate(&batch)?
                .into_array(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .clone();
            // add steps to state
            steps.push(arr);
        }

        // evaluate exclude
        let mut exclude = Some(vec![]);
        if let Some(exprs) = &self.exclude_expr {
            for expr in exprs.iter() {
                let arr = expr
                    .expr
                    .evaluate(&batch)?
                    .into_array(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone();
                if let Some(exclude) = &mut exclude {
                    let excl = Exclude {
                        exists: arr,
                        steps: expr.steps.clone(),
                    };
                    exclude.push(excl);
                } else {
                    let excl = Exclude {
                        exists: arr,
                        steps: expr.steps.clone(),
                    };
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
            .unwrap()
            .clone();

        let res = Batch {
            steps,
            exclude,
            constants,
            ts,
            batch,
        };

        Ok(res)
    }

    // entry point
    pub fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<FunnelResult>> {
        // evaluate each batch, e.g. create batch state
        let batches = record_batches
            .iter()
            .map(|b| self.evaluate_batch(b))
            .collect::<Result<Vec<_>>>()?;

        self.cur_span = 0;

        // if skip is set, we need to skip the first span
        let spans = if skip > 0 {
            let spans = [vec![skip], spans].concat();
            self.next_span(&batches, &spans);
            spans
        } else {
            spans
        };

        let mut results = vec![];
        let (window, filter) = (self.window.clone(), self.filter.clone());
        let mut dbg: Vec<DebugInfo> = vec![];
        // iterate over spans. For simplicity all ids are tied to span and start at 0
        while let Some(mut span) = self.next_span(&batches, &spans) {
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
                } else if !span.is_first_step() && span.time_window() > window.num_milliseconds() {
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
            let is_completed = match &filter {
                // if no filter, then funnel is completed id all steps are completed
                None => span.is_completed(),
                Some(filter) => match filter {
                    Filter::DropOffOnAnyStep => !span.is_last_step(),
                    // drop off on defined step
                    Filter::DropOffOnStep(drop_off_step_id) => span.step_id == *drop_off_step_id,
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
                false => FunnelResult::Incomplete(span.steps[0..=span.stepn].to_vec(), span.stepn),
            };

            results.push(fr);
        }

        self.dbg = dbg;

        Ok(results)
    }

    // take next span if exist
    fn next_span<'a>(&'a mut self, batches: &'a [Batch], spans: &[usize]) -> Option<Span> {
        if self.cur_span == spans.len() {
            return None;
        }

        let span_len = spans[self.cur_span];
        // offset is a sum of prev spans
        let offset = (0..self.cur_span).into_iter().map(|i| spans[i]).sum();
        let rows_count = batches.iter().map(|b| b.len()).sum::<usize>();
        if offset + span_len > rows_count {
            (
                " offset {offset}, span len: {span_len} > rows count: {}",
                rows_count,
            );
            return None;
        }
        self.cur_span += 1;
        Some(Span::new(
            self.cur_span - 1,
            offset,
            span_len,
            &self.steps,
            batches,
        ))
    }
}

pub mod test_utils {
    use std::cmp::max;
    use std::cmp::min;
    use std::sync::Arc;

    use anyhow::bail;
    use arrow::array;
    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use arrow2::array::Utf8Array;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::unicode_expressions::right;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::binary_expr;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::Expr;
    use datafusion_expr::Operator;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;
    use tracing::debug;
    use tracing::info;
    use tracing::log::Level::Debug;
    use tracing_core::Level;
    use tracing_test::traced_test;

    use super::Batch;
    use super::Count;
    use super::DebugInfo;
    use super::ExcludeExpr;
    use super::ExcludeSteps;
    use super::Filter;
    use super::FunnelExpr;
    use super::FunnelResult;
    use super::LoopResult;
    use super::LoopResult::*;
    use super::Options;
    use super::Step;
    use super::StepExpr;
    use super::StepOrder;
    use super::StepOrder::Any;
    use super::StepOrder::Sequential;
    use super::Touch;
    use crate::error::Result;

    pub fn get_sample_events(data: &str) -> (Vec<ArrayRef>, SchemaRef) {
        // todo change to arrow1
        let fields = vec![
            arrow2::datatypes::Field::new(
                "ts",
                arrow2::datatypes::DataType::Timestamp(
                    arrow2::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                false,
            ),
            arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
            arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
        ];
        let res = parse_markdown_table(data, &fields).unwrap();

        let (arrs, fields) = res
            .into_iter()
            .zip(fields)
            .map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap())
            .unzip();

        let schema = Arc::new(Schema::new(fields)) as SchemaRef;

        (arrs, schema)
    }

    pub fn event_eq_(
        schema: &Schema,
        event: &str,
        order: StepOrder,
    ) -> (PhysicalExprRef, StepOrder) {
        let l = Column::new_with_schema("event", schema).unwrap();
        let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
        let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
        (Arc::new(expr) as PhysicalExprRef, order)
    }

    pub fn evaluate_funnel(
        opts: Options,
        batch: RecordBatch,
        spans: Vec<usize>,
        exp: Vec<DebugInfo>,
        full_debug: bool,
        split_by: usize,
    ) -> Result<Vec<FunnelResult>> {
        let mut funnel = FunnelExpr::new(opts);

        let step = batch.num_rows() / split_by;
        let batches = (0..batch.num_rows())
            .into_iter()
            .step_by(step)
            .map(|o| {
                let mut n = step;
                if o + n > batch.num_rows() {
                    n = batch.num_rows() - o;
                }
                let cols = batch
                    .columns()
                    .iter()
                    .map(|col| col.slice(o, n).clone())
                    .collect::<Vec<_>>();
                RecordBatch::try_new(batch.schema().clone(), cols).unwrap()
            })
            .collect::<Vec<_>>();

        let res = funnel.evaluate(&batches, spans, 0)?;
        let i = 0;
        let exp_len = exp.len();
        assert_eq!(funnel.dbg.len(), exp_len);
        for (idx, info) in exp.into_iter().enumerate() {
            if full_debug {
                assert_eq!(funnel.dbg[idx], info);
            } else {
                assert_eq!(funnel.dbg[idx].loop_result, info.loop_result);
            }
        }
        Ok(res)
    }

    #[macro_export]
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
            ])
        }
    }
    #[macro_export]
    macro_rules! event_eq {
    ($schema:expr, $($name:literal $order:expr),+) => {
        vec![
            $(event_eq_($schema.as_ref(),$name,$order),)+
            ]
        }
    }
}

pub struct FunnelExprWrap {
    funnel: Mutex<FunnelExpr>,
}

impl FunnelExprWrap {
    pub fn new(expr: FunnelExpr) -> Self {
        Self {
            funnel: Mutex::new(expr),
        }
    }
}

impl PartitionedAggregateExpr for FunnelExprWrap {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut funnel = self.funnel.lock().unwrap();

        let res = funnel.evaluate(batches, spans, skip)?;
        let mut completed_builder = BooleanBuilder::with_capacity(res.len());
        let mut steps_builder = Int64Builder::with_capacity(res.len());
        let mut steps_ts_bulders = (0..funnel.steps.len())
            .into_iter()
            .map(|_| TimestampMillisecondBuilder::with_capacity(res.len()))
            .collect::<Vec<_>>();

        for row in res {
            match row {
                FunnelResult::Completed(steps) => {
                    completed_builder.append_value(true);
                    steps_builder.append_value(steps.len() as i64);
                    for (i, step) in steps.into_iter().enumerate() {
                        steps_ts_bulders[i].append_value(step.ts);
                    }
                }
                FunnelResult::Incomplete(steps, c) => {
                    completed_builder.append_value(false);
                    steps_builder.append_value(c as i64);
                    for i in 0..funnel.steps.len() {
                        if c > i {
                            steps_ts_bulders[i].append_value(steps[i].ts);
                        } else {
                            steps_ts_bulders[i].append_null();
                        }
                    }
                }
            }
        }

        let mut ret = vec![
            Arc::new(completed_builder.finish()) as ArrayRef,
            Arc::new(steps_builder.finish()) as ArrayRef,
        ];

        for b in steps_ts_bulders.iter_mut() {
            ret.push(Arc::new(b.finish()) as ArrayRef)
        }

        Ok(ret)
    }

    fn fields(&self) -> Vec<Field> {
        let funnel = self.funnel.lock().unwrap();
        let mut fields = vec![
            Field::new("completed", DataType::Boolean, false),
            Field::new("steps", DataType::Int64, false),
        ];

        let mut steps_ts_fields = funnel
            .steps
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                Field::new(
                    format!("step{idx}_ts"),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )
            })
            .collect::<Vec<_>>();

        fields.append(&mut steps_ts_fields);

        println!("{:?}", fields);
        fields
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(self.fields()))
    }
}

#[cfg(test)]
pub mod tests {
    use std::cmp::max;
    use std::cmp::min;
    use std::sync::Arc;

    use anyhow::bail;
    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use arrow::array::TimestampMillisecondArray;
    use arrow::compute::take;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::unicode_expressions::right;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::binary_expr;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::Expr;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_table;
    use tracing::debug;
    use tracing::info;
    use tracing::log::Level::Debug;
    use tracing_core::Level;
    use tracing_test::traced_test;

    use super::Batch;
    use super::Count;
    use super::DebugInfo;
    use super::ExcludeExpr;
    use super::ExcludeSteps;
    use super::Filter;
    use super::FunnelExpr;
    use super::FunnelResult;
    use super::LoopResult;
    use super::LoopResult::*;
    use super::Options;
    use super::Step;
    use super::StepExpr;
    use super::StepOrder;
    use super::StepOrder::Any;
    use super::StepOrder::Sequential;
    use super::Touch;
    use crate::error::Result;
    use crate::event_eq;
    use crate::expected_debug;
    use crate::physical_plan::expressions::funnel::test_utils::evaluate_funnel;
    use crate::physical_plan::expressions::funnel::test_utils::event_eq_;
    use crate::physical_plan::expressions::funnel::test_utils::get_sample_events;

    #[derive(Debug, Clone)]
    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        spans: Vec<usize>,
        exp: Vec<FunnelResult>,
        exp_debug: Option<Vec<DebugInfo>>,
        full_debug: bool,
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_sequential(1, 1),
                    Step::new_sequential(2, 2),
                ])],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep),
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(0, 0), Step::new_sequential(1, 1)],
                    1,
                )],
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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![4],
                exp_debug: expected_debug!(NextRow NextStep NextStep NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(1, 1),
                    Step::new_sequential(2, 2),
                    Step::new_sequential(3, 3),
                ])],
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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![5],
                exp_debug: expected_debug!(NextStep NextStep NextRow NextRow NextStep),
                // exp:  vec![FunnelResult { ts: vec![0, 1, 4], is_completed: true }],
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_sequential(1, 1),
                    Step::new_sequential(4, 4),
                ])],
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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: Some(vec![ExcludeExpr {
                        expr: event_eq_(schema.as_ref(), "ex", StepOrder::Sequential).0,
                        steps: None,
                    }]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![7],
                exp_debug: expected_debug!(NextStep NextStep ExcludeViolation NextRow NextStep NextStep NextStep),
                // exp:  vec![FunnelResult { ts: vec![4, 5, 6], is_completed: true }],
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(4, 4),
                    Step::new_sequential(5, 5),
                    Step::new_sequential(6, 6),
                ])],

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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: Some(vec![ExcludeExpr {
                        expr: event_eq_(schema.as_ref(), "e1", Sequential).0,
                        steps: Some(vec![ExcludeSteps::new(1, 2)]),
                    }]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![6],
                exp_debug: expected_debug!(NextStep NextStep ExcludeViolation NextStep NextStep NextStep),
                // exp:  vec![FunnelResult { ts: vec![4, 5, 6], is_completed: true }],
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(4, 3),
                    Step::new_sequential(5, 4),
                    Step::new_sequential(6, 5),
                ])],
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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: Some(vec![
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e1", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(1, 2)]),
                        },
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e2", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(0, 1)]),
                        },
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e3", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(1, 2)]),
                        },
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![9],
                exp_debug: expected_debug!(NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow),
                // exp:  None,,
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(7, 6)],
                    0,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: Some(vec![Column::new("const", 2)]),
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep ConstantViolation NextRow NextRow),
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(0, 0)],
                    0,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: Some(vec![Column::new("const", 2)]),
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                // exp:  vec![FunnelResult { ts: vec![0, 1, 2], is_completed: true }],
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_sequential(1, 1),
                    Step::new_sequential(2, 2),
                ])],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnAnyStep),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Incomplete(
                    vec![
                        Step::new_sequential(0, 0),
                        Step::new_sequential(1, 1),
                        Step::new_sequential(2, 2),
                    ],
                    2,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnAnyStep),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow),
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(0, 0), Step::new_sequential(1, 1)],
                    1,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnStep(1)),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Incomplete(
                    vec![
                        Step::new_sequential(0, 0),
                        Step::new_sequential(1, 1),
                        Step::new_sequential(2, 2),
                    ],
                    2,
                )],
                spans: vec![3],
                full_debug: false,
            },
            TestCase {
                name:
                    "failed funnel on drop on step 1 with drop of on step 1 filter should success"
                        .to_string(),
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::DropOffOnStep(1)),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow),
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(0, 0), Step::new_sequential(1, 1)],
                    1,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep OutOfWindow NextRow NextRow NextStep NextStep NextRow NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(6, 3),
                    Step::new_sequential(7, 4),
                    Step::new_sequential(9, 6),
                ])],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::TimeToConvert(
                        Duration::milliseconds(2),
                        Duration::milliseconds(10),
                    )),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_sequential(3, 1),
                    Step::new_sequential(5, 2),
                ])],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: Some(Filter::TimeToConvert(
                        Duration::milliseconds(4),
                        Duration::milliseconds(5),
                    )),
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Incomplete(
                    vec![
                        Step::new_sequential(0, 0),
                        Step::new_sequential(1, 1),
                        Step::new_sequential(2, 2),
                    ],
                    2,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep NextStep NextStep NextStep),
                exp: vec![
                    FunnelResult::Completed(vec![
                        Step::new_sequential(0, 0),
                        Step::new_sequential(1, 1),
                        Step::new_sequential(2, 2),
                    ]),
                    FunnelResult::Completed(vec![
                        Step::new_sequential(3, 0),
                        Step::new_sequential(4, 1),
                        Step::new_sequential(5, 2),
                    ]),
                ],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextRow NextStep NextStep NextStep),
                exp: vec![
                    FunnelResult::Incomplete(
                        vec![Step::new_sequential(0, 0), Step::new_sequential(1, 1)],
                        1,
                    ),
                    FunnelResult::Completed(vec![
                        Step::new_sequential(3, 0),
                        Step::new_sequential(4, 1),
                        Step::new_sequential(5, 2),
                    ]),
                ],
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
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: Some(vec![
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e1", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(1, 2)]),
                        },
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e2", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(0, 1)]),
                        },
                        ExcludeExpr {
                            expr: event_eq_(schema.as_ref(), "e3", Sequential).0,
                            steps: Some(vec![ExcludeSteps::new(1, 2)]),
                        },
                    ]),
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                spans: vec![18],
                exp_debug: expected_debug!(NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow NextStep ExcludeViolation NextRow),
                exp: vec![FunnelResult::Incomplete(
                    vec![Step::new_sequential(4, 15)],
                    0,
                )],
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
                    steps: event_eq!(schema, "e1" Sequential, "e2" Sequential , "e3" Sequential),

                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_sequential(1, 1),
                    Step::new_sequential(2, 2),
                ])],
                spans: vec![3, 4],
                full_debug: false,
            },
            TestCase {
                name: "any ordering".to_string(),
                data: r#"
| ts  | event | const  |
|-----|-------|--------|
| 0   | e1    | 1      |
| 1   | e3    | 1      |
| 2   | e2    | 1      |
| 3   | e4    | 1      |
| 4   | e6    | 1      |
| 5   | e5    | 1      |
| 6   | e7    | 1      |
| 7   | e8    | 1      |
"#,
                opts: Options {
                    ts_col: Column::new("ts", 0),
                    window: Duration::seconds(15),
                    steps: vec![
                        event_eq_(&schema, "e1", Sequential),
                        event_eq_(&schema, "e2", Any(vec![1, 2])),
                        event_eq_(&schema, "e3", Any(vec![1, 2])),
                        event_eq_(&schema, "e4", Sequential),
                        event_eq_(&schema, "e5", Any(vec![4, 5])),
                        event_eq_(&schema, "e6", Any(vec![4, 5])),
                        event_eq_(&schema, "e7", Sequential),
                        event_eq_(&schema, "e8", Any(vec![7])),
                    ],
                    exclude: None,
                    constants: None,
                    count: Count::Unique,
                    filter: None,
                    touch: Touch::First,
                },
                exp_debug: expected_debug!(NextStep NextStep NextStep NextStep NextStep NextStep NextStep NextStep),
                exp: vec![FunnelResult::Completed(vec![
                    Step::new_sequential(0, 0),
                    Step::new_any(1, 1, vec![1, 2]),
                    Step::new_any(2, 2, vec![1, 2]),
                    Step::new_sequential(3, 3),
                    Step::new_any(4, 4, vec![4, 5]),
                    Step::new_any(5, 5, vec![4, 5]),
                    Step::new_sequential(6, 6),
                    Step::new_any(7, 7, vec![7]),
                ])],
                spans: vec![8],
                full_debug: false,
            },
        ];

        let run_only: Option<&str> = None;
        for split_by in 1..3 {
            println!("split batches by {split_by}");
            for case in cases.iter().cloned() {
                if let Some(name) = run_only {
                    if case.name != name {
                        continue;
                    }
                }
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
