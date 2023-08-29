use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use axum::async_trait;
use common::query::Operator;
use datafusion::execution::context::QueryPlanner as DFQueryPlanner;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::ExtensionPlanner as DFExtensionPlanner;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion_common::DFSchema;
use datafusion_common::DataFusionError;
use datafusion_common::ExprSchema;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::Int8;
use datafusion_common::ToDFSchema;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::error::Result;
use crate::expr::property_col;
use crate::logical_plan;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::partitioned_aggregate::funnel::ExcludeExpr;
use crate::logical_plan::partitioned_aggregate::funnel::Filter;
use crate::logical_plan::partitioned_aggregate::funnel::StepOrder;
use crate::logical_plan::partitioned_aggregate::funnel::Touch;
use crate::logical_plan::partitioned_aggregate::AggregateExpr;
use crate::logical_plan::partitioned_aggregate::SegmentedAggregateNode;
use crate::logical_plan::partitioned_aggregate::SortField;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::segment::SegmentNode;
// use crate::logical_plan::_segmentation::AggregateFunction;
// use crate::logical_plan::_segmentation::SegmentationNode;
// use crate::logical_plan::_segmentation::TimeRange;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::expressions::segmentation::aggregate::{Aggregate, AggregateFunction};
// use crate::physical_plan::expressions::aggregate::aggregate;
// use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;
use crate::physical_plan::expressions::aggregate::partitioned;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::segmentation::boolean_op;
use crate::physical_plan::expressions::segmentation::comparison::And;
use crate::physical_plan::expressions::segmentation::comparison::Or;
use crate::physical_plan::expressions::segmentation::count::Count;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentExpr;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::pivot::PivotExec;
use crate::physical_plan::planner::build_filter;
use crate::physical_plan::planner::planner::col;
use crate::physical_plan::segment::SegmentExec;
use crate::physical_plan::segmented_aggregate::SegmentedAggregateExec;
use crate::physical_plan::unpivot::UnpivotExec;

fn aggregate<T>(agg: &logical_plan::segment::AggregateFunction) -> AggregateFunction<T> {
    match agg {
        logical_plan::segment::AggregateFunction::Sum => AggregateFunction::new_sum(),
        logical_plan::segment::AggregateFunction::Min => AggregateFunction::new_min(),
        logical_plan::segment::AggregateFunction::Max => AggregateFunction::new_max(),
        logical_plan::segment::AggregateFunction::Avg => AggregateFunction::new_avg(),
        logical_plan::segment::AggregateFunction::Count => {
            AggregateFunction::new_count()
        }
    }
}


fn build_time_range(time_range: logical_plan::segment::TimeRange) -> TimeRange {
    match time_range {
        logical_plan::segment::TimeRange::Between(a, b) => TimeRange::Between(a, b),
        logical_plan::segment::TimeRange::From(f) => TimeRange::From(f),
        logical_plan::segment::TimeRange::Last(x, y) => TimeRange::Last(x, y),
        logical_plan::segment::TimeRange::None => TimeRange::None,
    }
}

pub fn build_segment_expr(
    expr: logical_plan::segment::SegmentExpr,
    schema: &Schema,
) -> Result<Arc<dyn SegmentExpr>> {
    match expr {
        logical_plan::segment::SegmentExpr::And(l, r) => {
            let expr = And::new(
                build_segment_expr(*l, schema)?,
                build_segment_expr(*r, schema)?,
            );
            Ok(Box::new(expr) as Arc<dyn PartitionedAggregateExpr>)
        }
        logical_plan::segment::SegmentExpr::Or(l, r) => {
            let expr = Or::new(
                build_segment_expr(*l, schema)?,
                build_segment_expr(*r, schema)?,
            );
            Ok(Box::new(expr) as Arc<dyn PartitionedAggregateExpr>)
        }
        logical_plan::segment::SegmentExpr::Count {
            filter,
            ts_col,
            time_range,
            op,
            right,
            time_window,
        } => {
            let dfschema = schema.clone().to_dfschema()?;
            let execution_props = ExecutionProps::new();
            let filter = build_filter(Some(filter), &dfschema, schema, &execution_props)?.unwrap();

            let expr = match op {
                logical_plan::segment::Operator::Eq => Arc::new(Count::<boolean_op::Eq>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                )) as Arc<dyn SegmentExpr>,
                logical_plan::segment::Operator::NotEq => Arc::new(Count::<boolean_op::NotEq>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                ))
                    as Arc<dyn SegmentExpr>,
                logical_plan::segment::Operator::Lt => Arc::new(Count::<boolean_op::Lt>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                )) as Arc<dyn SegmentExpr>,
                logical_plan::segment::Operator::LtEq => Arc::new(Count::<boolean_op::LtEq>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                )) as Arc<dyn SegmentExpr>,
                logical_plan::segment::Operator::Gt => Arc::new(Count::<boolean_op::Gt>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                )) as Arc<dyn SegmentExpr>,
                logical_plan::segment::Operator::GtEq => Arc::new(Count::<boolean_op::GtEq>::new(
                    filter,
                    col(ts_col, &dfschema),
                    right,
                    build_time_range(time_range),
                    time_window,
                    10_000,
                )) as Arc<dyn SegmentExpr>,
            };
            Ok(expr)
        }
        logical_plan::segment::SegmentExpr::Aggregate {
            filter,
            predicate,
            ts_col,
            time_range,
            agg,
            op,
            right,
            time_window,
        } => {
            let dfschema = schema.clone().to_dfschema()?;
            let execution_props = ExecutionProps::new();
            let filter = build_filter(Some(filter), &dfschema, schema, &execution_props)?.unwrap();

            let expr = match op {
                logical_plan::segment::Operator::Eq => {
                    match right.get_datatype() {
                        DataType::Int8 => {
                            Arc::new(Aggregate::<i8, i32, boolean_op::Gt>::new(
                                filter,
                                col(ts_col, &dfschema),
                                col(predicate, &dfschema),
                                aggregate(&agg),
                                right.into(),
                                build_time_range(time_range),
                                time_window,
                                10_000,
                            )) as Arc<dyn SegmentExpr>,
                        }
                        DataType::Int16 => {}
                        DataType::Int32 => {}
                        DataType::Int64 => {}
                        DataType::UInt8 => {}
                        DataType::UInt16 => {}
                        DataType::UInt32 => {}
                        DataType::UInt64 => {}
                        DataType::Float16 => {}
                        DataType::Float32 => {}
                        DataType::Float64 => {}
                        DataType::Decimal128(_, _) => {}
                        _ => unimplemented!()
                    }
                }
                logical_plan::segment::Operator::NotEq => {}
                logical_plan::segment::Operator::Lt => {}
                logical_plan::segment::Operator::LtEq => {}
                logical_plan::segment::Operator::Gt => {}
                logical_plan::segment::Operator::GtEq => {}
            };

            Ok(expr)
        }
    }
}
