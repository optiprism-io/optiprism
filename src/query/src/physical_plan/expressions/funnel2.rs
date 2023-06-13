use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, mpsc};
use arrow::array::{Array, BooleanArray, Int64Array, TimestampMillisecondArray, UInt64Array};
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
use super::*;
use tracing_core::Level;
use crate::{StaticArray};
use std::sync::mpsc::{Sender, Receiver};
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion_common::Result as DFResult;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StepOrder {
    Sequential,
    Any(Vec<usize>),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Step {
    pub ts: i64,
    pub row_id: usize,
    pub order: StepOrder,
}

impl Step {
    pub fn new(ts: i64, row_id: usize, order: StepOrder) -> Self {
        Self { ts, row_id, order }
    }
    pub fn new_sequential(ts: i64, row_id: usize) -> Self {
        Self { ts, row_id, order: StepOrder::Sequential }
    }

    pub fn new_any(ts: i64, row_id: usize, any: Vec<usize>) -> Self {
        Self { ts, row_id, order: StepOrder::Any(any) }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FunnelResult {
    Completed(Vec<Step>),
    Incomplete(Vec<Step>, usize),
}

pub enum Report {
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
pub struct Exclude {
    exists: BooleanArray,
    steps: Option<Vec<ExcludeSteps>>,
}

#[derive(Debug, Clone)]
pub struct Batch {
    pub steps: Vec<BooleanArray>,
    // pointer to step_results
    pub exclude: Option<Vec<Exclude>>,
    pub constants: Option<Vec<StaticArray>>,
    pub ts: TimestampMillisecondArray,
    pub batch: RecordBatch,
}

#[derive(Debug, Clone, Default)]
struct Row {
    batch_id: usize,
    row_id: usize,
}

#[derive(Debug, Clone)]
pub struct Span {
    offset: usize,
    len: usize,
    batches: VecDeque<Batch>,
    const_row: Option<Row>,
    steps: Vec<Step>,
    step_id: usize,
    row_id: usize,
    first_step: bool,
    stepn: usize,
}

impl Span {
    pub fn new(offset: usize, len: usize, steps: &[StepOrder], batches: VecDeque<Batch>) -> Self {
        let const_row = if batches[0].constants.is_some() {
            Some(Row::default())
        } else {
            None
        };

        Self {
            offset,
            len,
            batches,
            const_row,
            steps: steps.iter().map(|step| Step::new(0, 0, step.to_owned())).collect(),
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
        for batch in self.batches.iter() {
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
        let (batch_id, idx) = self.abs_row_id();
        let mut del_step = None;
        match &mut self.steps[self.step_id].order {
            StepOrder::Sequential => {
                return if del_step.is_none() {
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

impl Batch {
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
    partition_expr: Vec<PhysicalExprRef>,
    buf: VecDeque<Batch>,
    hash_buffer: Vec<u64>,
    random_state: ahash::RandomState,
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps: Vec<StepOrder>,
    span_offset: usize,
    span_len: usize,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    last_value: Option<u64>,
    last_span: usize,
    pub results: Vec<FunnelResult>,
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
    NextRow,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub ts_col: Column,
    pub window: Duration,
    pub partition_expr: Vec<PhysicalExprRef>,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
}

pub enum Error {
    OutOfWindow
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DebugInfo {
    loop_result: LoopResult,
    step_id: usize,
    row_id: usize,
    ts: i64,
}

impl FunnelExpr {
    pub fn new(opts: Options) -> Self {
        Self {
            partition_expr: opts.partition_expr,
            buf: VecDeque::new(),
            hash_buffer: vec![],
            random_state: Default::default(),
            ts_col: opts.ts_col,
            window: opts.window,
            steps_expr: opts.steps.iter().map(|(expr, _)| expr.clone()).collect::<Vec<_>>(),
            steps: opts.steps.iter().map(|(_, order)| order.clone()).collect::<Vec<_>>(),
            span_offset: 0,
            span_len: 0,
            exclude_expr: opts.exclude,
            constants: opts.constants,
            last_value: None,
            last_span: 0,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            dbg: vec![],
            results: vec![],
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }
    fn evaluate_batch<'a>(&mut self, batch: RecordBatch) -> Result<Batch> {
        let mut steps = vec![];
        for expr in self.steps_expr.iter() {
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

    fn evaluate_partition(&mut self, len: usize) -> Result<FunnelResult> {
        println!("offset: {}, len: {}", self.span_offset, self.span_len);
        let mut span = Span::new(self.span_offset - len, len, &self.steps, self.buf.clone());

        // iterate over spans. For simplicity all ids are tied to span and start at 0
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

        Ok(fr)
    }

    pub fn evaluate(&mut self, batch: RecordBatch) -> Result<()> {
        // calculate partition key hashes
        let arrays = self.partition_expr
            .iter()
            .map(|expr| {
                Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
            })
            .collect::<DFResult<Vec<_>>>()?;

        // perf: pre-allocate hash buffer
        let mut hash_buffer = vec![0; batch.num_rows()];
        create_hashes(&arrays, &self.random_state, &mut hash_buffer)?;
        let our_batch = self.evaluate_batch(batch.clone())?;
        self.buf.push_back(our_batch.clone());

        println!("ev");
        for (idx, v) in hash_buffer.iter().enumerate() {
            if self.last_value.is_none() {
                self.last_value = Some(*v);
            }

            if self.last_value != Some(*v) {
                if idx==0 && self.buf.len()>1 {
                    // let v = self.buf.pop_front().unwrap();
                    // println!("{:?}",v);
                    println!("offset:{}",self.span_offset);
                    // self.span_offset-=v.len()-1;
                    println!("{}",self.span_offset);
                }
                println!("idx:{}",idx);
                let res = self.evaluate_partition(self.span_len)?;
                println!("res: {:?}", res);
                self.span_len = 0;
                self.last_value = Some(*v);
                self.results.push(res);
            }
            self.span_len += 1;
            self.span_offset += 1;
            self.last_value = Some(*v);
        };

        Ok(())
    }

    pub fn finalize(&mut self) -> Result<Option<FunnelResult>> {
        if self.span_len > 0 {
            let res = self.evaluate_partition(self.span_len)?;
            println!("res: {:?}", res);
            Ok(Some(res))
        } else { Ok(None) }
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;
    use arrow::array::ArrayRef;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;
    use crate::physical_plan::expressions::funnel2::{Count, FunnelExpr, Options, StepOrder, Touch};
    use super::StepOrder::{Sequential, Any};

    pub fn get_sample_events(data: &str) -> (Vec<ArrayRef>, SchemaRef) {
        // todo change to arrow1
        let fields = vec![
            arrow2::datatypes::Field::new("user_id", arrow2::datatypes::DataType::UInt64, false),
            arrow2::datatypes::Field::new("ts", arrow2::datatypes::DataType::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None), false),
            arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
            arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
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

    #[macro_export]
    macro_rules! event_eq {
    ($schema:expr, $($name:literal $order:expr),+) => {
        vec![
            $(event_eq_($schema.as_ref(),$name,$order),)+
            ]
        }
    }

    pub fn event_eq_(schema: &Schema, event: &str, order: StepOrder) -> (PhysicalExprRef, StepOrder) {
        let l = Column::new_with_schema("event", schema).unwrap();
        let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
        let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
        (Arc::new(expr) as PhysicalExprRef, order)
    }

    #[test]
    fn it_works() {
        let fields = vec![
            Field::new("user_id", DataType::UInt64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("event", DataType::Utf8, true),
            Field::new("const", DataType::Int64, true),
        ];
        let schema = Arc::new(Schema::new(fields)) as SchemaRef;

        let opts = Options {
            ts_col: Column::new_with_schema("ts", schema.as_ref()).unwrap(),
            window: Duration::seconds(15),
            partition_expr: vec![Arc::new(Column::new_with_schema("user_id", schema.as_ref()).unwrap()) as PhysicalExprRef],
            steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),

            exclude: None,
            constants: None,
            count: Count::Unique,
            filter: None,
            touch: Touch::First,
        };

        let input1 = r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 1       | 0  | e1    | 1     |
| 1       | 1  | e2    | 1     |
| 1       | 2  | e3    | 1     |
"#;

        let input2 = r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 2       | 4  | e1    | 1     |
| 2       | 5  | e2    | 1     |
| 2       | 6  | e3    | 1     |
| 3       | 7  | e1    | 1     |
| 3       | 8  | e2    | 1     |
"#;

        let input3 = r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 3       | 9  | e3    | 1     |
| 4       | 10 | e1    | 1     |
| 4       | 11 | e2    | 1     |
| 4       | 12 | e3    | 1     |
"#;
        let (arrs, schema) = get_sample_events(input1);
        let batch1 = RecordBatch::try_new(schema, arrs).unwrap();
        let (arrs, schema) = get_sample_events(input2);
        let batch2 = RecordBatch::try_new(schema, arrs).unwrap();
        let (arrs, schema) = get_sample_events(input3);
        let batch3 = RecordBatch::try_new(schema, arrs).unwrap();

        let mut funnel = FunnelExpr::new(opts);
        funnel.evaluate(batch1).unwrap();
        funnel.evaluate(batch2).unwrap();
        funnel.evaluate(batch3).unwrap();
        funnel.finalize().unwrap();
    }
}