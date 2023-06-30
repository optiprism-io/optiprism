use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::datatypes::DataType;
use axum::async_trait;
use datafusion::execution::context::QueryPlanner as DFQueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
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
// use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
// use crate::physical_plan::expressions::segmentation::count::Count;
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

macro_rules! agg_expr {
    ($filter:expr,$predicate_expr:expr,$ts_col:expr,$time_range:expr,$t:ty,$acc:ty,$agg_fn:ident,$builder:ident) => {
        Arc::new(
            Aggregate::<$t, $acc, _>::try_new(
                $filter,
                $predicate_expr,
                SegmentAggregateFunction::$agg_fn(),
                $builder::with_capacity(10_000),
                $ts_col,
                $time_range,
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?,
        ) as Arc<dyn SegmentationExpr>
    };
}

macro_rules! agg_segment_expr {
    ($filter:expr,$schema:expr,$predicate:expr,$agg_fn:ident,$ts_col:expr,$time_range:expr) => {{
        let predicate_expr = expressions::Column::new(
            $predicate.name.as_str(),
            $schema.index_of_column(&$predicate)?,
        );
        match $schema.data_type($predicate)? {
            DataType::Int8 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                i8,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::Int16 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                i16,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::Int32 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                i32,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::Int64 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                i64,
                i128,
                $agg_fn,
                Decimal128Builder
            ),
            DataType::UInt8 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                u8,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::UInt16 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                u16,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::UInt32 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                u32,
                i64,
                $agg_fn,
                Int64Builder
            ),
            DataType::UInt64 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                u64,
                i128,
                $agg_fn,
                Decimal128Builder
            ),
            DataType::Float32 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                f32,
                f64,
                $agg_fn,
                Float64Builder
            ),
            DataType::Float64 => agg_expr!(
                $filter,
                predicate_expr,
                $ts_col,
                $time_range,
                f64,
                f64,
                $agg_fn,
                Float64Builder
            ),
            DataType::Decimal128(p, s) => Arc::new(
                Aggregate::<Decimal128Array, i128, _>::try_new(
                    $filter,
                    predicate_expr,
                    SegmentAggregateFunction::$agg_fn(),
                    Decimal128Builder::with_capacity(10_000),
                    $ts_col,
                    $time_range,
                )
                .map_err(|err| DataFusionError::Plan(err.to_string()))?,
            ) as Arc<dyn SegmentationExpr>,
            _ => unimplemented!(),
        }
    }};
}

#[async_trait]
impl DFExtensionPlanner for ExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &SessionState,
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
        } /*else if let Some(node) = any.downcast_ref::<SegmentationNode>() {
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
                        TimeRange::Each(_) => unimplemented!()
                    },
                };
                let ts_col =
                    expressions::Column::new("ts", node.schema.index_of_column(&node.ts_col)?);
                let schema = node.input.schema();
                let filter = create_physical_expr(&expr.filter, schema.as_ref(), &physical_inputs[0].schema(), ctx_state.execution_props())?;
                let res = match &expr.agg_fn {
                    AggregateFunction::Sum(predicate) => {
                        agg_segment_expr!(filter,schema, predicate, new_sum, ts_col, time_range)
                    }
                    AggregateFunction::Min(predicate) => {
                        agg_segment_expr!(filter,schema, predicate, new_min, ts_col, time_range)
                    }
                    AggregateFunction::Max(predicate) => {
                        agg_segment_expr!(filter,schema, predicate, new_max, ts_col, time_range)
                    }
                    AggregateFunction::Avg(predicate) => {
                        agg_segment_expr!(filter,schema, predicate, new_avg, ts_col, time_range)
                    }
                    AggregateFunction::Count => {
                        Arc::new(Count::new(filter, ts_col, time_range)) as Arc<dyn SegmentationExpr>
                    }
                };
                exprs.push(res)
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

            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        }*/ else {
            None
        };
        Ok(plan)
    }
}

