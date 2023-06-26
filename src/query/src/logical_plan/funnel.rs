use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::Duration;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

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

#[derive(Clone, Debug)]
pub struct ExcludeExpr {
    expr: Expr,
    steps: Option<Vec<ExcludeSteps>>,
}

#[derive(Clone, Debug)]
pub enum Count {
    Unique,
    NonUnique,
    Session,
}

// Additional filter to complete
#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,                  // funnel should fail on any step
    DropOffOnStep(usize),              // funnel should fail on certain step
    TimeToConvert(Duration, Duration), // conversion should be within certain window
}

#[derive(Clone, Debug)]
pub enum Touch {
    First,
    Last,
    Step(usize),
}

#[derive(Hash, Eq, PartialEq)]
pub struct FunnelNode {
    ts_col: Column,
    steps: Vec<(Expr, StepOrder)>,
    exclude: Option<Vec<ExcludeExpr>>,
    count: Count,
    filter: Option<Filter>,
    touch: Touch,
    input: LogicalPlan,
    schema: DFSchemaRef,
}

pub struct Options {
    ts_col: Column,
    steps: Vec<(Expr, StepOrder)>,
    exclude: Option<Vec<ExcludeExpr>>,
    count: Count,
    filter: Option<Filter>,
    touch: Touch,
}
impl FunnelNode {
    pub fn try_new(input: LogicalPlan, opts: Options) -> Result<Self> {
        let schema = {
            let mut fields = vec![];
            fields.push(DFField::new_unqualified(
                "is_converted",
                DataType::Boolean,
                true,
            ));
            fields.push(DFField::new_unqualified(
                "converted_steps",
                DataType::UInt32,
                true,
            ));
            for step_id in 0..predicate.steps_count() {
                fields.push(DFField::new_unqualified(
                    format!("step_{step_id}_ts").as_str(),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ));
            }

            Arc::new(DFSchema::new_with_metadata(
                vec![fields, input.schema().fields().clone()].concat(),
                HashMap::new(),
            )?)
        };

        Ok(Self {
            ts_col: opts.ts_col,
            steps: opts.steps,
            exclude: opts.exclude,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            input,
            schema,
        })
    }
}

impl Debug for FunnelNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for FunnelNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Funnel"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Funnel")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        let opts = Options {
            ts_col: self.ts_col.clone(),
            steps: self.steps.clone(),
            exclude: self.exclude.clone(),
            count: self.count.clone(),
            filter: self.filter.clone(),
            touch: self.touch.clone(),
        };
        let funnel = FunnelNode::try_new(inputs[0].clone(), opts)
            .map_err(QueryError::into_datafusion_plan_error)
            .unwrap();

        Arc::new(funnel)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,

            None => false,
        }
    }
}
