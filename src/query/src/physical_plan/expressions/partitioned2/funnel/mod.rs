use arrow::array::BooleanArray;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::error::Result;
use crate::StaticArray;
// mod trends_grouped;
mod funnel;

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

// Batch for state
#[derive(Debug, Clone)]
pub struct Batch {
    pub steps: Vec<BooleanArray>,
    // boolean Exists array for each step
    // pointer to step_results
    pub exclude: Option<Vec<Exclude>>,
    // optional exclude with Exists array
    pub constants: Option<Vec<StaticArray>>,
    // optional constants
    pub ts: TimestampMillisecondArray,
    // timestamp
    pub batch: RecordBatch, // ref to actual record batch
}

impl Batch {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

// calculate expressions
fn evaluate_batch(
    batch: RecordBatch,
    steps_expr: &Vec<PhysicalExprRef>,
    exclude_expr: &Option<Vec<ExcludeExpr>>,
    constants: &Option<Vec<Column>>,
    ts_col: &Column,
) -> Result<Batch> {
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
