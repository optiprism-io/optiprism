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
use datafusion_common::ScalarValue;
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
use crate::physical_plan::expressions::segmentation::boolean_op;
use crate::physical_plan::expressions::segmentation::boolean_op::*;
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

macro_rules! agg_expr {
    ($filter:expr,$left:expr,$ts_col:expr,$agg_fn:ident,$typ:ty,$acc_typ:ty,$scalar_ty:tt,$op:ident,$right:expr,$time_range:expr,$time_window:expr) => {
        Arc::new(
            Aggregate::<$typ, $acc_typ, $op>::try_new(
                $filter,
                $left,
                SegmentAggregateFunction::$agg_fn(),
                $ts_col,
                scalar_to!($typ, $right),
                $time_range,
                $time_window,
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?,
        ) as Arc<dyn SegmentationExpr>
    };
}

macro_rules! scalar_to {
    ($typ:tt,$right:expr) => {
        scalar_to_$typ($right)
    };
}

macro_rules! scalar_to_primitive {
    ($scalar_ty:tt,$typ:tt) => {
        fn scalar_to_$typ(scalar: ScalarValue) -> $typ {
            match scalar {
                ScalarValue::$scalar_ty(Some(v)) => v,
                _ => panic!("Invalid scalar type"),
            }
        }
    };
}

scalar_to_primitive!(Int8, i8);
scalar_to_primitive!(Int16, i16);
scalar_to_primitive!(Int32, i32);
scalar_to_primitive!(Int64, i64);
scalar_to_primitive!(UInt8, i8);
scalar_to_primitive!(UInt16, i16);
scalar_to_primitive!(UInt32, i32);
scalar_to_primitive!(UInt64, i64);
scalar_to_primitive!(Float16, f16);
scalar_to_primitive!(Float32, f32);
scalar_to_primitive!(Float64, f64);
macro_rules! agg_segment_expr {
    ($schema:expr, $filter:expr, $ts_col:expr, $agg_fn:ident, $left:expr,$op:ident,$right:expr, $time_range:expr,$time_window:expr) => {{
        let left_expr =
            expressions::Column::new($left.name.as_str(), $schema.index_of_column(&$left)?);
        match $schema.data_type(&$left)? {
            DataType::Int8 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                i8,
                i64,
                Int8,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::Int16 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                i16,
                i64,
                Int16,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::Int32 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                i32,
                i64,
                Int32,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::Int64 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                i64,
                i128,
                Int64,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::UInt8 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                u8,
                i64,
                UInt8,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::UInt16 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                u16,
                i64,
                UInt16,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::UInt32 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                u32,
                i64,
                UInt32,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::UInt64 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                u64,
                i128,
                UInt64,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::Float32 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                f32,
                f64,
                Float32,
                $op,
                $right,
                $time_range,
                $time_window
            ),
            DataType::Float64 => agg_expr!(
                $filter,
                left_expr,
                $ts_col,
                $agg_fn,
                f64,
                f64,
                Float64,
                $op,
                $right,
                $time_range,
                $time_window
            )/*,
            DataType::Decimal128(p, s) => Arc::new(
                agg_expr!(
                    $filter,
                    left_expr,
                    $ts_col,
                    $agg_fn,
                    Decimal128Array,
                    i128,
                    $op,
                    $right,
                    $time_range,
                    $time_window
                )
                .map_err(|err| DataFusionError::Plan(err.to_string()))?,
            ) as Arc<dyn SegmentationExpr>,*/
            _ => unimplemented!(),
        }
    }};
}

macro_rules! agg_segment_expr_outer {
    ($schema:expr, $filter:expr, $ts_col:expr, $agg_fn:ident, $left:expr,$op:expr,$right:expr, $time_range:expr,$time_window:expr) => {{
        match $op {
            Operator::Eq => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                Eq,
                $right,
                $time_range,
                $time_window
            ),
            Operator::NotEq => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                NotEq,
                $right,
                $time_range,
                $time_window
            ),
            Operator::Lt => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                Lt,
                $right,
                $time_range,
                $time_window
            ),
            Operator::LtEq => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                LtEq,
                $right,
                $time_range,
                $time_window
            ),
            Operator::Gt => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                Gt,
                $right,
                $time_range,
                $time_window
            ),
            Operator::GtEq => agg_segment_expr!(
                $schema,
                $filter,
                $ts_col,
                $agg_fn,
                $left,
                GtEq,
                $right,
                $time_range,
                $time_window
            ),
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
        } else if let Some(node) = any.downcast_ref::<SegmentationNode>() {
            let mut exprs: Vec<Arc<dyn SegmentationExpr>> = Vec::with_capacity(node.exprs.len());
            for expr in node.exprs.iter() {
                let time_range = match &expr.time_range {
                    TimeRange::Between(from, to) => SegmentTimeRange::Between(*from, *to),
                    TimeRange::From(from) => SegmentTimeRange::From(*from),
                    TimeRange::Last(last, ts) => SegmentTimeRange::Last(*last, *ts),
                    TimeRange::None => SegmentTimeRange::None,
                };
                let ts_col =
                    expressions::Column::new("ts", node.schema.index_of_column(&node.ts_col)?);
                let schema = node.input.schema();
                let filter = create_physical_expr(
                    &expr.filter,
                    schema.as_ref(),
                    &physical_inputs[0].schema(),
                    ctx_state.execution_props(),
                )?;
                let res = match &expr.agg_fn {
                    AggregateFunction::Sum(agg) => agg_segment_expr_outer!(
                        schema,
                        filter,
                        ts_col,
                        new_sum,
                        agg.left,
                        agg.op,
                        agg.right,
                        time_range,
                        expr.time_window
                    ),
                    AggregateFunction::Min(agg) => agg_segment_expr_outer!(
                        schema,
                        filter,
                        ts_col,
                        new_min,
                        agg.left,
                        agg.op,
                        agg.right,
                        time_range,
                        expr.time_window
                    ),
                    AggregateFunction::Max(agg) => agg_segment_expr_outer!(
                        schema,
                        filter,
                        ts_col,
                        new_max,
                        agg.left,
                        agg.op,
                        agg.right,
                        time_range,
                        expr.time_window
                    ),
                    AggregateFunction::Avg(agg) => agg_segment_expr_outer!(
                        schema,
                        filter,
                        ts_col,
                        new_avg,
                        agg.left,
                        agg.op,
                        agg.right,
                        time_range,
                        expr.time_window
                    ),
                    AggregateFunction::Count { op, right } => match op {
                        Operator::Eq => Arc::new(Count::<boolean_op::Eq>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        Operator::NotEq => Arc::new(Count::<boolean_op::NotEq>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        Operator::Lt => Arc::new(Count::<boolean_op::Lt>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        Operator::LtEq => Arc::new(Count::<boolean_op::LtEq>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        Operator::Gt => Arc::new(Count::<boolean_op::Gt>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        Operator::GtEq => Arc::new(Count::<boolean_op::GtEq>::new(
                            filter,
                            ts_col,
                            *right,
                            time_range,
                            expr.time_window,
                        )),
                        _ => unimplemented!(),
                    },
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
        } else {
            None
        };
        Ok(plan)
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<ScalarValue> for $ty {
            fn from(value: ScalarValue) -> Self {
                if let ScalarValue::$scalar(Some(v)) = value {
                    v
                } else {
                    panic!("invalid scalar value")
                }
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
