use std::ops::Deref;
use std::sync::Arc;
use datafusion::execution::context::{ExecutionContextState, QueryPlanner as DFQueryPlanner};
use datafusion::physical_plan::planner::{
    DefaultPhysicalPlanner, ExtensionPlanner as DFExtensionPlanner,
};

use datafusion::logical_plan::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, expressions, PhysicalPlanner};
use datafusion::{
    error::{DataFusionError, Result},
    physical_plan::displayable,
};
use crate::logical_plan::merge::MergeNode;
use crate::physical_plan::merge::MergeExec;
use axum::{async_trait};
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::pivot::PivotExec;
use crate::physical_plan::unpivot::UnpivotExec;

pub struct QueryPlanner {}

#[async_trait]
impl DFQueryPlanner for QueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state).await
    }
}

pub struct ExtensionPlanner {}

impl DFExtensionPlanner for ExtensionPlanner {
    fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(_) = any.downcast_ref::<MergeNode>() {
            let exec = MergeExec::try_new(physical_inputs.to_vec()).map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<UnpivotNode>() {
            let exec = UnpivotExec::try_new(
                physical_inputs[0].clone(),
                node.cols.clone(),
                node.name_col.clone(),
                node.value_col.clone(),
            ).map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<PivotNode>() {
            let schema = node.input.schema();
            let exec = PivotExec::try_new(
                physical_inputs[0].clone(),
                expressions::Column::new(node.name_col.name.as_str(), schema.index_of_column(&node.name_col)?),
                expressions::Column::new(node.value_col.name.as_str(), schema.index_of_column(&node.value_col)?),
                node.result_cols.clone(),
            ).map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<DictionaryDecodeNode>() {
            let schema = node.input.schema();
            let decode_cols = node.decode_cols
                .iter()
                .map(|(col, dict)| expressions::Column::new(
                    col.name.as_str(),
                    schema.index_of_column(col).unwrap()
                ))
                .collect::<Result<_>>()?;
            let exec = DictionaryDecodeExec::new(physical_inputs[0].clone(), decode_cols);
        } else {
            None
        };
        Ok(plan)
    }
}
