use datafusion::execution::context::{ExecutionContext, ExecutionConfig};

use datafusion::execution::context::{QueryPlanner as DFQueryPlanner, ExecutionContextState};
use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner, PhysicalExpr};
use std::sync::Arc;
use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner as DFExtensionPlanner};
use datafusion::error::Result;
use crate::logical_plan::segment_join::JoinPlanNode;
use crate::physical_plan::segment_join::{JoinExec, Segment};
use datafusion::physical_plan::expressions::Column;

pub fn make_context() -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_query_planner(Arc::new(QueryPlanner {}))
        .with_concurrency(48);

    ExecutionContext::with_config(config)
}

struct QueryPlanner {}

impl DFQueryPlanner for QueryPlanner {
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                ExtensionPlanner {},
            )]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

struct ExtensionPlanner {}

impl DFExtensionPlanner for ExtensionPlanner {
    fn plan_extension(&self,
                      planner: &dyn PhysicalPlanner,
                      node: &dyn UserDefinedLogicalNode,
                      logical_inputs: &[&LogicalPlan],
                      physical_inputs: &[Arc<dyn ExecutionPlan>],
                      ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(join) = any.downcast_ref::<JoinPlanNode>() {
            assert_eq!(physical_inputs.len(), 2, "Inconsistent number of inputs");

            let segments = join.segments.iter().map(|x| {
                let left_expr = if let Some(expr) = &x.left_expr {
                    Some(planner.create_physical_expr(
                        expr,
                        &logical_inputs[0].schema(),
                        &physical_inputs[0].schema(),
                        ctx_state,
                    )?)
                } else {
                    None
                };

                Ok(Segment {
                    left_expr,
                    right_expr: x.right_expr.clone(),
                })
            }).collect::<Result<Vec<Segment>>>()?;
            Some(Arc::new(JoinExec::try_new(
                Arc::clone(&physical_inputs[0]),
                Arc::clone(&physical_inputs[1]),
                (
                    Column::new(&join.on.0.name, join.left.schema().index_of_column(&join.on.0)?),
                    Column::new(&join.on.1.name, join.right.schema().index_of_column(&join.on.1)?),
                ),
                segments,
                join.schema().as_ref().clone().into(),
                join.target_batch_size,
            )?) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}