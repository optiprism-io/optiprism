use arrow::array::BooleanArray;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::error::Result;
use crate::StaticArray;

pub mod funnel;
pub mod funnel_trend;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StepOrder {
    Sequential,
    Any(Vec<usize>), // any of the steps
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

pub enum Order {
    Any,
    Asc,
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

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

// calculate expressions
fn evaluate_batch<'a>(
    batch: &'a RecordBatch,
    steps_expr: &Vec<PhysicalExprRef>,
    exclude_expr: &Option<Vec<ExcludeExpr>>,
    constants: &Option<Vec<Column>>,
    ts_col: &Column,
) -> Result<Batch<'a>> {
    let mut steps = vec![];
    // evaluate steps
    for expr in steps_expr.iter() {
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
    if let Some(exprs) = &exclude_expr {
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
    let mut constants_out: Option<Vec<_>> = None;
    if let Some(cc) = constants {
        for c in cc.iter() {
            let arr = c.evaluate(&batch)?.into_array(0);
            let arr = StaticArray::from(arr);
            if let Some(c) = &mut constants_out {
                c.push(arr)
            } else {
                constants_out = Some(vec![arr])
            }
        }
    }
    // timestamp column
    // Optiprism uses millisecond precision
    let ts = ts_col
        .evaluate(&batch)?
        .into_array(0)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
        .clone();

    let res = Batch {
        steps,
        exclude,
        constants: constants_out,
        ts,
        batch,
    };

    Ok(res)
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
    // fixed const row. This will be filled with field occurrence if constants are enabled and
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

    #[inline]
    pub fn is_next_row(&self) -> bool {
        return self.row_id + 1 <= self.len - 1;
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

// take next span if exist
fn next_span<'a>(
    batches: &'a [Batch],
    spans: &[usize],
    cur_span: &mut usize,
    steps: &[StepOrder],
) -> Option<Span<'a>> {
    if *cur_span == spans.len() {
        return None;
    }

    let span_len = spans[*cur_span];
    // offset is a sum of prev spans
    let offset = (0..*cur_span).into_iter().map(|i| spans[i]).sum();
    let rows_count = batches.iter().map(|b| b.len()).sum::<usize>();
    if offset + span_len > rows_count {
        (
            " offset {offset}, span len: {span_len} > rows count: {}",
            rows_count,
        );
        return None;
    }
    *cur_span += 1;
    Some(Span::new(*cur_span - 1, offset, span_len, steps, batches))
}

// Result of the funnel
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FunnelResult {
    // completed result with all the steps
    Completed(Vec<Step>),
    // incompleted result with completed steps and count of completed steps
    Incomplete(Vec<Step>, usize),
}
