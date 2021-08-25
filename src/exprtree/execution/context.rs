use datafusion::execution::context::{ExecutionContext, ExecutionConfig};

use datafusion::execution::context::{ExecutionContextState};
use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner, PhysicalExpr};
use std::sync::Arc;
use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner as DFExtensionPlanner};
use datafusion::error::Result;
use crate::exprtree::logical_plan::segment_join::JoinPlanNode;
use crate::exprtree::physical_plan::segment_join::{JoinExec, Segment};
use datafusion::physical_plan::expressions::Column;
use crate::exprtree::execution::planner::{QueryPlanner};

pub fn make_context() -> ExecutionContext {
    let config = ExecutionConfig::new()
        .with_query_planner(Arc::new(QueryPlanner {}))
        .with_concurrency(48);

    ExecutionContext::with_config(config)
}
