use datafusion::execution::context::{QueryPlanner, ExecutionContextState};
use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode, Expr, DFSchemaRef};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use std::sync::Arc;
use datafusion::optimizer::optimizer::OptimizerRule;
use std::fmt::Formatter;
use std::any::Any;
use datafusion::logical_plan::Column;
struct JoinQueryPlanner {}

struct JoinOptimizerRule {}

struct JoinPlanNode {
    left: LogicalPlan,
    right: LogicalPlan,
    on: Vec<(Column, Column)>,
}

struct JoinPlanner {}
/*
impl QueryPlanner for JoinQueryPlanner {
    fn create_physical_plan(&self, logical_plan: &LogicalPlan, ctx_state: &ExecutionContextState) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                JoinPlanner {},
            )]);
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

impl UserDefinedLogicalNode for JoinPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        todo!()
    }

    fn schema(&self) -> &DFSchemaRef {
        todo!()
    }

    fn expressions(&self) -> Vec<Expr> {
        todo!()
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'a>) -> Result {
        todo!()
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        todo!()
    }
}*/