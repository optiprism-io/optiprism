use arrow::array::BooleanArray;
use chrono::Duration;
use datafusion::physical_expr::PhysicalExprRef;

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
