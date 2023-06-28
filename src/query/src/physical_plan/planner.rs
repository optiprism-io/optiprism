use std::sync::Arc;

use arrow::array::Float64Builder;
use arrow::datatypes::DataType;
use axum::async_trait;
use datafusion::execution::context::QueryPlanner as DFQueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::planner::ExtensionPlanner as DFExtensionPlanner;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalPlanner;
use datafusion_common::DataFusionError;
use datafusion_common::ExprSchema;
use datafusion_common::Result;
use datafusion_common::Result as DFResult;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::segmentation::AggregateFunction;
use crate::logical_plan::segmentation::SegmentationNode;
use crate::logical_plan::segmentation::TimeRange;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
use crate::physical_plan::expressions::segmentation::count::Count;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange as SegmentTimeRange;
use crate::physical_plan::expressions::segmentation::AggregateFunction as SegmentAggregateFunction;
use crate::physical_plan::expressions::segmentation::SegmentationExpr;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::pivot::PivotExec;
use crate::physical_plan::segmentation;
use crate::physical_plan::segmentation::SegmentationExec;
use crate::physical_plan::unpivot::UnpivotExec;
pub struct QueryPlanner {}

#[async_trait]
impl DFQueryPlanner for QueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ExtensionPlanner {})]);
        physical_planner
            .create_physical_plan(logical_plan, ctx_state)
            .await
    }
}

pub struct ExtensionPlanner {}

#[async_trait]
impl DFExtensionPlanner for ExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _ctx_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if any.downcast_ref::<MergeNode>().is_some() {
            let exec = MergeExec::try_new(physical_inputs.to_vec())
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
            let exec = DictionaryDecodeExec::new(physical_inputs[0].clone(), decode_cols);
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        /*} else if let Some(node) = any.downcast_ref::<SegmentationNode>() {
            let mut exprs: Vec<Arc<dyn SegmentationExpr>> = Vec::with_capacity(node.exprs.len());
            for expr in node.exprs.iter() {
                let time_range = match &expr.time_range {
                    None => SegmentTimeRange::None,
                    Some(v) => match v {
                        TimeRange::Between(from, to) => SegmentTimeRange::Between(*from, *to),
                        TimeRange::From(from) => SegmentTimeRange::From(*from),
                        TimeRange::Last(since, start_ts) => {
                            SegmentTimeRange::Last(*since, *start_ts)
                        }
                    },
                };
                let ts_col =
                    expressions::Column::new("ts", node.schema.index_of_column(&node.ts_col)?);
                let res = match &expr.agg_fn {
                    AggregateFunction::Sum(predicate) => {
                        agg_segment_expr!(node.input.schema(), predicate, new_sum)
                    }
                    AggregateFunction::Min(_) => {
                        agg_segment_expr!(node.input.schema(), predicate, new_min)
                    }
                    AggregateFunction::Max(_) => {
                        agg_segment_expr!(node.input.schema(), predicate, new_max)
                    }
                    AggregateFunction::Avg(_) => {
                        agg_segment_expr!(node.input.schema(), predicate, new_avg)
                    }
                    AggregateFunction::Count => Count::new(ts_col, time_range),
                };
                exprs.push(Arc::new(res))
            }

            let cols = node
                .partition_cols
                .iter()
                .map(|c| {
                    expressions::Column::new(
                        c.name.as_str(),
                        node.schema.index_of_column(c).unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            let exec = SegmentationExec::try_new(exprs, cols, physical_inputs[0].clone(), 100)
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;

            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)*/
        } else {
            None
        };
        Ok(plan)
    }
}
// macro_rules! agg_expr {
// ($t:typ,$acc:typ,$agg_fn:ident,$builder:ident) => {
// Aggregate::<$t, $acc, _>::try_new(
// predicate_expr,
// SegmentAggregateFunction::$agg_fn(),
// $builder::with_capacity(10_000),
// ts_col,
// time_range,
// )?
// };
// }
// macro_rules! agg_segment_expr {
// ($schema:expr,$predicate:expr,$agg_fn:ident) => {
// let predicate_expr = expressions::Column::new(
// $predicate.name.as_str(),
// $schema.index_of_column(&predicate)?,
// );
// match $schema.data_type(predicate)? {
// DataType::Int8 => agg_expr!(i8, i64, $agg_fn, Int64Builder),
// DataType::Int16 => agg_expr!(i16, i64, $agg_fn, Int64Builder),
// DataType::Int32 => agg_expr!(i32, i64, $agg_fn, Int64Builder),
// DataType::Int64 => agg_expr!(i32, i128, $agg_fn, Decimal128Builder),
// DataType::UInt8 => agg_expr!(u8, i64, $agg_fn, Int64Builder),
// DataType::UInt16 => agg_expr!(u16, i64, $agg_fn, Int64Builder),
// DataType::UInt32 => agg_expr!(u32, i64, $agg_fn, Int64Builder),
// DataType::UInt64 => agg_expr!(u64, i64, $agg_fn, Int64Builder),
// DataType::Float32 => agg_expr!(f32, f64, $agg_fn, Float64Builder),
// DataType::Float64 => agg_expr!(f64, f64, $agg_fn, Float64Builder),
// DataType::Dictionary(p, s) => {}
// DataType::Decimal128(_, _) => {}
// DataType::Decimal256(_, _) => {}
// DataType::Map(_, _) => {}
// DataType::RunEndEncoded(_, _) => {}
// }
// };
// }
