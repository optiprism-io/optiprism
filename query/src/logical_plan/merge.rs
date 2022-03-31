use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use datafusion::logical_plan::{LogicalPlan, DFSchemaRef, UserDefinedLogicalNode};
use datafusion_expr::Expr;

pub struct MergeNode {
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
}

impl MergeNode {
    pub fn new(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>) -> Self {
        Self {
            left,
            right,
        }
    }
}

impl Debug for MergeNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for MergeNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.inpu]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Merge: left={:?}, right:{:?}", self.left, self.right)
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        Arc::new(Self {
            left: Arc::new(inputs[0].clone()),
            right: Arc::new(inputs[1].clone()),
        })
    }
}