use datafusion::execution::context::{QueryPlanner, ExecutionContextState};
use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode, Expr, DFSchemaRef};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use std::sync::Arc;
use datafusion::optimizer::optimizer::OptimizerRule;
use std::fmt::{Formatter, Debug};
use std::any::Any;
use datafusion::logical_plan::Column;
use crate::segment::expressions::multibatch::expr::Expr as SegmentExpr;
use arrow::datatypes::SchemaRef;
use std::fmt;
use datafusion::error::Result;

pub type JoinOn = (Column, Column);

#[derive(Debug, Clone)]
pub struct Segment {
    pub left_expr: Option<Expr>,
    pub right_expr: Option<Arc<dyn SegmentExpr>>,
}


pub struct JoinPlanNode {
    pub segments: Vec<Segment>,
    pub left: LogicalPlan,
    pub right: LogicalPlan,
    pub on: JoinOn,
    pub take_left_cols: Option<Vec<usize>>,
    pub take_right_cols: Option<Vec<usize>>,
    pub schema: DFSchemaRef,
    pub segment_names: bool,
    pub target_batch_size: usize,
}

impl Debug for JoinPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for JoinPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "SegmentJoin: {} = {}", self.on.0.name, self.on.1.name)?;
        for (id, segment) in self.segments.iter().enumerate() {
            if let Some(expr) = &segment.left_expr {
                write!(f, " #{:?} left: ", expr)?;
            }
            if let Some(expr) = &segment.right_expr {
                write!(f, " #{:?} right: ", expr)?;
            }
        }
        Ok(())
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        panic!("unimplemented");
    }
}