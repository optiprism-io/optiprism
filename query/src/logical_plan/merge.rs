use datafusion::logical_plan::{DFSchemaRef, LogicalPlan, UserDefinedLogicalNode};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::error::QueryError;

use crate::Result;

pub struct MergeNode {
    inputs: Vec<LogicalPlan>,
    schema: DFSchemaRef,
}

impl MergeNode {
    pub fn try_new(inputs: Vec<LogicalPlan>) -> Result<Self> {
        let mut schema = DFSchema::new(vec![])?;
        for input in inputs.iter() {
            schema.merge(input.schema());
        }
        Ok(Self {
            inputs,
            schema: Arc::new(schema),
        })
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
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Merge")
    }

    fn from_template(
        &self,
        _: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        Arc::new(
            MergeNode::try_new(inputs.to_vec())
                .map_err(QueryError::into_datafusion_plan_error)
                .unwrap(),
        )
    }
}
