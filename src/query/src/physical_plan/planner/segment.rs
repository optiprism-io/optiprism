use std::sync::Arc;

use arrow::array::Decimal128Array;
use arrow::datatypes::Schema;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion_common::ScalarValue;
use datafusion_common::ToDFSchema;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

use crate::error::Result;
use crate::logical_plan;
// use crate::logical_plan::_segmentation::AggregateFunction;
// use crate::logical_plan::_segmentation::SegmentationNode;
// use crate::logical_plan::_segmentation::TimeRange;

// use crate::physical_plan::expressions::aggregate::aggregate;
// use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;

// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;
use crate::physical_plan::expressions::segmentation::boolean_op::*;
use crate::physical_plan::expressions::segmentation::comparison::And;
use crate::physical_plan::expressions::segmentation::comparison::Or;
use crate::physical_plan::expressions::segmentation::count::Count;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentExpr;
use crate::physical_plan::planner::build_filter;
use crate::physical_plan::planner::planner::col;

fn aggregate<T>(agg: &logical_plan::segment::AggregateFunction) -> AggregateFunction<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display {
    match agg {
        logical_plan::segment::AggregateFunction::Sum => AggregateFunction::new_sum(),
        logical_plan::segment::AggregateFunction::Min => AggregateFunction::new_min(),
        logical_plan::segment::AggregateFunction::Max => AggregateFunction::new_max(),
        logical_plan::segment::AggregateFunction::Avg => AggregateFunction::new_avg(),
        logical_plan::segment::AggregateFunction::Count => AggregateFunction::new_count(),
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

macro_rules! count {
    ($op:ident,$filter:expr,$ts_col:expr,$right:expr,$time_range:expr,$time_window:expr) => {
        Arc::new(Count::<$op>::new(
            $filter,
            $ts_col,
            $right,
            $time_range,
            $time_window,
            10_000,
        )) as Arc<dyn SegmentExpr>
    };
}

macro_rules! _aggregate {
    ($t1:ident,$t2:ident, $op:ty, $filter:expr,$ts_col:expr,$predicate_col:expr,$agg:expr,$right:expr,$time_range:expr,$time_window:expr) => {
        Arc::new(Aggregate::<$t1, $t2, $op>::new(
            $filter,
            $ts_col,
            $predicate_col,
            $agg,
            $right,
            $time_range,
            $time_window,
            10_000,
        )) as Arc<dyn SegmentExpr>
    };
}
macro_rules! aggregate {
    ($op:ty, $filter:expr,$ts_col:expr,$predicate_col:expr,$agg:expr,$right:expr,$time_range:expr,$time_window:expr) => {
        match $right {
            // ScalarValue::Int8(Some(v)) => _aggregate!(
            // i8,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::Int16(Some(v)) => _aggregate!(
            // i16,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::Int32(Some(v)) => _aggregate!(
            // i32,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            ScalarValue::Int64(Some(v)) => _aggregate!(
                i64,
                i128,
                $op,
                $filter,
                $ts_col,
                $predicate_col,
                $agg,
                v as i128,
                $time_range,
                $time_window
            ),
            // ScalarValue::UInt8(Some(v)) => _aggregate!(
            // u8,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::UInt16(Some(v)) => _aggregate!(
            // u16,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::UInt32(Some(v)) => _aggregate!(
            // u32,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i64,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::UInt64(Some(v)) => _aggregate!(
            // u64,
            // u128,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // v as i128,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::Float32(Some(v)) => _aggregate!(
            // f32,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // $right,
            // $time_range,
            // $time_window
            // ),
            // ScalarValue::Float64(Some(v)) => _aggregate!(
            // f64,
            // i64,
            // $op,
            // $filter,
            // $ts_col,
            // $predicate_col,
            // $agg,
            // $right,
            // $time_range,
            // $time_window
            // ),
            ScalarValue::Decimal128(Some(v), _, _) => _aggregate!(
                Decimal128Array,
                i128,
                $op,
                $filter,
                $ts_col,
                $predicate_col,
                $agg,
                v,
                $time_range,
                $time_window
            ),
            _ => unimplemented!(),
        }
    };
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
            Ok(Arc::new(expr) as Arc<dyn SegmentExpr>)
        }
        logical_plan::segment::SegmentExpr::Or(l, r) => {
            let expr = Or::new(
                build_segment_expr(*l, schema)?,
                build_segment_expr(*r, schema)?,
            );
            Ok(Arc::new(expr) as Arc<dyn SegmentExpr>)
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
            let ts_col = col(ts_col, &dfschema);
            let time_range = build_time_range(time_range);
            let expr = match op {
                logical_plan::segment::Operator::Eq => {
                    count!(Eq, filter, ts_col, right, time_range, time_window)
                }
                logical_plan::segment::Operator::NotEq => {
                    count!(NotEq, filter, ts_col, right, time_range, time_window)
                }
                logical_plan::segment::Operator::Lt => {
                    count!(Lt, filter, ts_col, right, time_range, time_window)
                }
                logical_plan::segment::Operator::LtEq => {
                    count!(LtEq, filter, ts_col, right, time_range, time_window)
                }
                logical_plan::segment::Operator::Gt => {
                    count!(Gt, filter, ts_col, right, time_range, time_window)
                }
                logical_plan::segment::Operator::GtEq => {
                    count!(GtEq, filter, ts_col, right, time_range, time_window)
                }
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
            let ts_col = col(ts_col, &dfschema);
            let predicate_col = col(predicate, &dfschema);

            let time_range = build_time_range(time_range);

            let agg = aggregate(&agg);
            let expr = match op {
                logical_plan::segment::Operator::Eq => {
                    aggregate!(
                        Eq,
                        filter,
                        ts_col,
                        predicate_col,
                        agg,
                        right,
                        time_range,
                        time_window
                    )
                }
                logical_plan::segment::Operator::NotEq => aggregate!(
                    NotEq,
                    filter,
                    ts_col,
                    predicate_col,
                    agg,
                    right,
                    time_range,
                    time_window
                ),
                logical_plan::segment::Operator::Lt => aggregate!(
                    Lt,
                    filter,
                    ts_col,
                    predicate_col,
                    agg,
                    right,
                    time_range,
                    time_window
                ),
                logical_plan::segment::Operator::LtEq => aggregate!(
                    LtEq,
                    filter,
                    ts_col,
                    predicate_col,
                    agg,
                    right,
                    time_range,
                    time_window
                ),
                logical_plan::segment::Operator::Gt => aggregate!(
                    Gt,
                    filter,
                    ts_col,
                    predicate_col,
                    agg,
                    right,
                    time_range,
                    time_window
                ),
                logical_plan::segment::Operator::GtEq => aggregate!(
                    GtEq,
                    filter,
                    ts_col,
                    predicate_col,
                    agg,
                    right,
                    time_range,
                    time_window
                ),
            };

            Ok(expr)
        }
    }
}
