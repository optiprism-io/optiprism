use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::datatypes::DataType;
use axum::async_trait;
use common::query::Operator;
use datafusion::execution::context::QueryPlanner as DFQueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::planner::ExtensionPlanner as DFExtensionPlanner;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PhysicalPlanner;
use datafusion_common::DataFusionError;
use datafusion_common::ExprSchema;
use datafusion_common::Result;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::Int8;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::pivot::PivotNode;
// use crate::logical_plan::segmentation::AggregateFunction;
// use crate::logical_plan::segmentation::SegmentationNode;
// use crate::logical_plan::segmentation::TimeRange;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::expressions::partitioned::aggregate::Aggregate;
use crate::physical_plan::expressions::partitioned::boolean_op;
use crate::physical_plan::expressions::partitioned::boolean_op::*;
use crate::physical_plan::expressions::partitioned::comparison::And;
use crate::physical_plan::expressions::partitioned::comparison::Or;
use crate::physical_plan::expressions::partitioned::count::Count;
use crate::physical_plan::expressions::partitioned::time_range;
use crate::physical_plan::expressions::partitioned::time_range::TimeRange as SegmentTimeRange;
use crate::physical_plan::expressions::partitioned::AggregateFunction as SegmentAggregateFunction;
use crate::physical_plan::expressions::partitioned::SegmentationExpr;
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

macro_rules! aggregate_expr {
    ($schema:expr,$filter:expr,$left:expr,$agg_fn:ident,$right:expr,$time_range:expr,$time_window:expr) => {
        match $right {
            ScalarValue::Int64(Some(v)) => Arc::new(
                Aggregate::<i8, i64, Gt>::try_new(
                    $filter,
                    $left,
                    physical_plan::expressions::segmentation::AggregateFunction::$agg_fn(),
                    Column::new_with_schema("ts", $schema).unwrap(),
                    v,
                    $time_range,
                    $time_window,
                )
                .map_err(|err| DataFusionError::Plan(err.to_string()))?,
            ) as Arc<dyn SegmentationExpr>,
            _ => unimplemented!(),
        }
    };
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
            let mut segments = vec![];
            let mut exprs: Vec<Arc<dyn SegmentationExpr>> = Vec::with_capacity(node.exprs.len());
            for segment in node.segments.iter() {
                let mut or_expr = vec![];
                for exprs in segment.expressions.iter() {
                    let mut and_exprs = vec![];
                    for expr in exprs.iter() {
                        let time_range = match &expr.time_range {
                            TimeRange::Between(from, to) => SegmentTimeRange::Between(*from, *to),
                            TimeRange::From(from) => SegmentTimeRange::From(*from),
                            TimeRange::Last(last, ts) => SegmentTimeRange::Last(*last, *ts),
                            TimeRange::None => SegmentTimeRange::None,
                        };
                        let ts_col = expressions::Column::new(
                            "ts",
                            node.schema.index_of_column(&node.ts_col)?,
                        );
                        let schema = node.input.schema();
                        let filter = create_physical_expr(
                            &expr.filter,
                            schema.as_ref(),
                            &physical_inputs[0].schema(),
                            ctx_state.execution_props(),
                        )?;
                        let schema = &physical_inputs[0].schema();
                        let expr = match &expr.agg_fn {
                            AggregateFunction::Sum(agg) => {
                                let left_col =
                                    Column::new_with_schema(agg.left.name.as_str(), schema)?;
                                aggregate_expr!(
                                    schema,
                                    filter,
                                    left_col,
                                    new_sum,
                                    agg.right,
                                    time_range,
                                    expr.time_window
                                )
                            }
                            AggregateFunction::Min(agg) => {
                                let left_col =
                                    Column::new_with_schema(agg.left.name.as_str(), schema)?;
                                aggregate_expr!(
                                    schema,
                                    filter,
                                    left_col,
                                    new_min,
                                    agg.right,
                                    time_range,
                                    expr.time_window
                                )
                            }
                            AggregateFunction::Max(agg) => {
                                let left_col =
                                    Column::new_with_schema(agg.left.name.as_str(), schema)?;
                                aggregate_expr!(
                                    schema,
                                    filter,
                                    left_col,
                                    new_max,
                                    agg.right,
                                    time_range,
                                    expr.time_window
                                )
                            }
                            AggregateFunction::Avg(agg) => {
                                let left_col =
                                    Column::new_with_schema(agg.left.name.as_str(), schema)?;
                                aggregate_expr!(
                                    schema,
                                    filter,
                                    left_col,
                                    new_avg,
                                    agg.right,
                                    time_range,
                                    expr.time_window
                                )
                            }

                            AggregateFunction::Count { op, right } => match op {
                                Operator::Eq => Arc::new(Count::<boolean_op::Eq>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                Operator::NotEq => Arc::new(Count::<boolean_op::NotEq>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                Operator::Lt => Arc::new(Count::<boolean_op::Lt>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                Operator::LtEq => Arc::new(Count::<boolean_op::LtEq>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                Operator::Gt => Arc::new(Count::<boolean_op::Gt>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                Operator::GtEq => Arc::new(Count::<boolean_op::GtEq>::new(
                                    filter,
                                    ts_col,
                                    *right,
                                    time_range,
                                    expr.time_window,
                                ))
                                    as Arc<dyn SegmentationExpr>,
                                _ => unimplemented!(),
                            },
                        };
                        and_exprs.push(expr)
                    }
                    if and_exprs.len() > 1 {
                        for idx in (0..and_exprs.len() - 1).into_iter().step_by(2) {
                            and_exprs[0] = Arc::new(And::new(
                                and_exprs[idx].clone(),
                                and_exprs[idx + 1].clone(),
                            ));
                        }
                    }
                    or_expr.push(and_exprs[0].clone());
                }
                if or_expr.len() > 1 {
                    for idx in (0..or_expr.len() - 1).into_iter().step_by(2) {
                        or_expr[0] =
                            Arc::new(Or::new(or_expr[idx].clone(), or_expr[idx + 1].clone()));
                    }
                }
                segments.push(or_expr[0].clone());
            }

            let cols = node
                .partition_cols
                .iter()
                .map(|c| Column::new(c.name.as_str(), node.schema.index_of_column(c).unwrap()))
                .collect::<Vec<_>>();
            let exec = SegmentationExec::try_new(segments, cols, physical_inputs[0].clone(), 100)
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;

            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        }*/ else {
            None
        };
        Ok(plan)
    }
}
