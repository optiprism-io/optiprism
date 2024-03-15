use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::array::Int64Array;
use arrow::array::TimestampMillisecondArray;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions::Literal;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;

use crate::error::Result;
use crate::StaticArray;
#[allow(clippy::module_inception)]
pub mod funnel;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StepOrder {
    Sequential,
    Any(Vec<(usize, usize)>), // any of the steps
}

#[derive(Clone, Debug)]
pub struct ExcludeSteps {
    pub from: usize,
    pub to: usize,
}

impl ExcludeSteps {
    pub fn new(from: usize, to: usize) -> Self {
        Self { from, to }
    }
}

#[derive(Clone, Debug)]
pub struct ExcludeExpr {
    pub expr: PhysicalExprRef,
    pub steps: Option<ExcludeSteps>,
}

#[derive(Clone)]
pub struct StepExpr {
    _expr: PhysicalExprRef,
}

#[derive(Clone, Debug)]
pub enum Touch {
    First,
    Last,
    Step(usize),
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
    steps: Option<ExcludeSteps>,
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
    pub batch: RecordBatch,
    pub first_partition: i64,
}

impl Batch {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }
}

// calculate expressions
fn evaluate_batch(
    batch: RecordBatch,
    steps_expr: &[PhysicalExprRef],
    exclude_expr: &Option<Vec<ExcludeExpr>>,
    constants: &Option<Vec<PhysicalExprRef>>,
    ts_col: &PhysicalExprRef,
    partition_col: &PhysicalExprRef,
) -> Result<Batch> {
    let mut steps = vec![];
    // evaluate steps
    for expr in steps_expr.iter() {
        // evaluate expr to bool result
        let arr = expr
            .evaluate(&batch)?
            .into_array(0)?
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
                .into_array(0)?
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
            let arr = c.evaluate(&batch)?.into_array(0)?;
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
        .into_array(0)?
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap()
        .clone();
    let parr = partition_col
        .evaluate(&batch)?
        .into_array(batch.num_rows())?
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    let res = Batch {
        steps,
        exclude,
        constants: constants_out,
        ts,
        batch,
        first_partition: parr.value(0),
    };

    Ok(res)
}

pub fn event_eq_(schema: &Schema, event: &str, order: StepOrder) -> (PhysicalExprRef, StepOrder) {
    let l = Column::new_with_schema("event", schema).unwrap();
    let r = Literal::new(ScalarValue::Utf8(Some(event.to_string())));
    let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
    (Arc::new(expr) as PhysicalExprRef, order)
}

#[macro_export]
macro_rules! event_eq {
    ($schema:expr, $($name:literal $order:expr),+) => {
        vec![
            $(event_eq_($schema.as_ref(),$name,$order),)+
            ]
        }
    }

#[macro_export]
macro_rules! expected_debug {
    ($($ident:ident)+) => {
        vec![
            $(DebugStep::$ident,)+
            ]
    }
    }
