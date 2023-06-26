use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Hash, Debug, Clone, Eq, PartialEq)]
pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

#[derive(Hash, Eq, PartialEq)]
pub struct SegmentationNode {
    pub input: LogicalPlan,
    pub agg_fn: AggregateFunction,
    pub partition_key: Expr,
}

impl SegmentationNode {
    pub fn try_new(
        input: LogicalPlan,
        agg_fn: AggregateFunction,
        partition_key: Expr,
    ) -> Result<Self> {
        Ok(Self {
            input,
            agg_fn,
            partition_key,
        })
    }
}

impl Debug for SegmentationNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SegmentationNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Segmentation"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Segmentation")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            SegmentationNode::try_new(
                inputs[0].clone(),
                self.agg_fn.clone(),
                self.partition_key.clone(),
            )
            .map_err(QueryError::into_datafusion_plan_error)
            .unwrap(),
        )
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        use std::hash::Hash;
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
