use std::sync::Arc;
use std::sync::Mutex;

use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::expressions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::DFSchema;
use datafusion_common::DataFusionError;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

mod funnel;
mod partitioned_aggregate;
mod segment;

use datafusion::execution::context::QueryPlanner as DFQueryPlanner;
use datafusion::physical_planner::ExtensionPlanner as DFExtensionPlanner;
use datafusion_common::Result as DFResult;

use crate::error::Result;
use crate::logical_plan::add_string_column::AddStringColumnNode;
use crate::logical_plan::aggregate_columns::AggregateAndSortColumnsNode;
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::funnel::FunnelNode;
use crate::logical_plan::limit_groups::LimitGroupsNode;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode;
use crate::logical_plan::partitioned_aggregate::PartitionedAggregatePartialNode;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::rename_column_rows::RenameColumnRowsNode;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::logical_plan::reorder_columns::ReorderColumnsNode;
use crate::logical_plan::segment::SegmentNode;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan::add_string_column::AddStringColumnExec;
use crate::physical_plan::aggregate_and_sort_columns::AggregateAndSortColumnsExec;
use crate::physical_plan::db_parquet::DBParquetExec;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::limit_groups::LimitGroupsExec;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::pivot::PivotExec;
use crate::physical_plan::planner::funnel::build_funnel;
use crate::physical_plan::planner::partitioned_aggregate::build_partitioned_aggregate_final_expr;
use crate::physical_plan::planner::partitioned_aggregate::build_partitioned_aggregate_partial_expr;
use crate::physical_plan::planner::segment::build_segment_expr;
use crate::physical_plan::rename_column_rows::RenameColumnRowsExec;
use crate::physical_plan::rename_columns::RenameColumnsExec;
use crate::physical_plan::reorder_columns::ReorderColumnsExec;
use crate::physical_plan::segment::SegmentExec;
use crate::physical_plan::segmented_aggregate::SegmentedAggregateFinalExec;
use crate::physical_plan::segmented_aggregate::SegmentedAggregatePartialExec;
use crate::physical_plan::unpivot::UnpivotExec;

// use crate::logical_plan::_segmentation::AggregateFunction;
// use crate::logical_plan::_segmentation::SegmentationNode;
// use crate::logical_plan::_segmentation::TimeRange;
// use crate::physical_plan::expressions::aggregate::aggregate;
// use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;
// use crate::physical_plan::expressions::aggregate::count::Count;

// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;

fn build_filter(
    filter: Option<Expr>,
    dfschema: &DFSchema,
    schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let ret = filter
        .map(|e| create_physical_expr(&e, dfschema, execution_props))
        .transpose()?;

    Ok(ret)
}

pub struct QueryPlanner {}

#[async_trait]
impl DFQueryPlanner for QueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ExtensionPlanner {})]);
        physical_planner
            .create_physical_plan(logical_plan, ctx_state)
            .await
    }
}

pub struct ExtensionPlanner {}

pub fn col(col: datafusion_common::Column, dfschema: &DFSchema) -> Column {
    Column::new(col.name.as_str(), dfschema.index_of_column(&col).unwrap())
}

#[async_trait]
impl DFExtensionPlanner for ExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _ctx_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(node) = any.downcast_ref::<MergeNode>() {
            let exec = MergeExec::try_new(physical_inputs.to_vec(), node.names.clone())
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<AddStringColumnNode>() {
            let exec = AddStringColumnExec::new(physical_inputs[0].clone(), node.col.clone());
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<ReorderColumnsNode>() {
            let exec =
                ReorderColumnsExec::try_new(physical_inputs[0].clone(), node.columns.clone())
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<RenameColumnsNode>() {
            let exec = RenameColumnsExec::try_new(physical_inputs[0].clone(), node.columns.clone())
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<AggregateAndSortColumnsNode>() {
            let exec =
                AggregateAndSortColumnsExec::new(physical_inputs[0].clone(), node.groups.clone());
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<LimitGroupsNode>() {
            let exec = LimitGroupsExec::try_new(
                physical_inputs[0].clone(),
                node.skip,
                node.groups,
                node.limit,
            )
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<RenameColumnRowsNode>() {
            let col = Column::new(
                node.column.name.as_str(),
                logical_inputs[0].schema().index_of_column(&node.column)?,
            );

            let exec = RenameColumnRowsExec::try_new(
                physical_inputs[0].clone(),
                col,
                node.rename.to_vec(),
            )
            .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<DbParquetNode>() {
            let exec = DBParquetExec::try_new(node.db.clone(), node.projection.clone())
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<UnpivotNode>() {
            let exec = UnpivotExec::try_new(
                physical_inputs[0].clone(),
                node.cols.clone(),
                node.name_col.clone(),
                node.value_col.clone(),
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<PivotNode>() {
            let schema = node.input.schema();
            let exec = PivotExec::try_new(
                physical_inputs[0].clone(),
                expressions::Column::new(
                    node.name_col.name.as_str(),
                    schema.index_of_column(&node.name_col)?,
                ),
                expressions::Column::new(
                    node.value_col.name.as_str(),
                    schema.index_of_column(&node.value_col)?,
                ),
                node.result_cols.clone(),
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<DictionaryDecodeNode>() {
            let schema = node.input.schema();
            let decode_cols = node
                .decode_cols
                .iter()
                .map(|(col, dict)| {
                    (
                        expressions::Column::new(
                            col.name.as_str(),
                            schema.index_of_column(col).unwrap(),
                        ),
                        dict.to_owned(),
                    )
                })
                .collect();
            let exec = DictionaryDecodeExec::try_new(physical_inputs[0].clone(), decode_cols)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<FunnelNode>() {
            Some(Arc::new(
                build_funnel(logical_inputs, physical_inputs, node)
                    .map_err(|err| DataFusionError::Plan(err.to_string()))?,
            ) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<PartitionedAggregatePartialNode>() {
            let partition_inputs = node
                .partition_inputs
                .clone()
                .map(|c| physical_inputs[1..c.len()].to_vec());

            let partition_col = Column::new(
                node.partition_col.name.as_str(),
                logical_inputs[0]
                    .schema()
                    .index_of_column(&node.partition_col)?,
            );
            let agg_expr = node
                .agg_expr
                .clone()
                .into_iter()
                .map(|(expr, name)| {
                    build_partitioned_aggregate_partial_expr(expr, &physical_inputs[0].schema())
                        .map(|expr| (Arc::new(Mutex::new(expr)), name))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            let exec = SegmentedAggregatePartialExec::try_new(
                physical_inputs[0].clone(),
                partition_inputs,
                partition_col,
                agg_expr,
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<PartitionedAggregateFinalNode>() {
            let agg_expr = node
                .agg_expr
                .clone()
                .into_iter()
                .map(|(expr, name)| {
                    build_partitioned_aggregate_final_expr(expr, &physical_inputs[0].schema())
                        .map(|expr| (Arc::new(Mutex::new(expr)), name))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            let exec = SegmentedAggregateFinalExec::try_new(physical_inputs[0].clone(), agg_expr)
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else if let Some(node) = any.downcast_ref::<SegmentNode>() {
            let partition_col = Column::new(
                node.partition_col.name.as_str(),
                node.schema.index_of_column(&node.partition_col)?,
            );
            let segment_expr = build_segment_expr(node.expr.clone(), &physical_inputs[0].schema())
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            let exec = SegmentExec::try_new(
                physical_inputs[0].clone(),
                segment_expr,
                partition_col,
                // todo define out_buffer_size
                10_000,
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}
