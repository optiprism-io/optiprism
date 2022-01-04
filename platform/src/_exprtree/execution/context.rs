use crate::exprtree::execution::planner::QueryPlanner;
use crate::exprtree::logical_plan::segment_join::JoinPlanNode;
use crate::exprtree::physical_plan::segment_join::{JoinExec, Segment};
use datafusion::error::Result;
use datafusion::execution::context::ExecutionContextState;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::planner::{
    DefaultPhysicalPlanner, ExtensionPlanner as DFExtensionPlanner,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, PhysicalPlanner};
use std::sync::Arc;

pub fn make_context() -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_query_planner(Arc::new(QueryPlanner {}))
        .with_concurrency(48);

    ExecutionContext::with_config(config)
}
