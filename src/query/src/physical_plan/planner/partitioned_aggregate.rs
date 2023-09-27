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
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
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
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

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
use crate::logical_plan::partitioned_aggregate::PartitionedAggregateNode;
use crate::logical_plan::partitioned_aggregate::SortField;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::segment::SegmentNode;
// use crate::logical_plan::_segmentation::AggregateFunction;
// use crate::logical_plan::_segmentation::SegmentationNode;
// use crate::logical_plan::_segmentation::TimeRange;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::physical_plan;
use crate::physical_plan::dictionary_decode::DictionaryDecodeExec;
use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;
use crate::physical_plan::expressions::aggregate::count::Count;
// use crate::physical_plan::expressions::aggregate::aggregate;
// use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;
// use crate::physical_plan::expressions::aggregate::count::Count;
use crate::physical_plan::expressions::aggregate::partitioned;
use crate::physical_plan::expressions::aggregate::partitioned::count::PartitionedCount;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;
use crate::physical_plan::merge::MergeExec;
use crate::physical_plan::pivot::PivotExec;
use crate::physical_plan::planner::build_filter;
use crate::physical_plan::planner::planner::col;
use crate::physical_plan::segment::SegmentExec;
use crate::physical_plan::segmented_aggregate::SegmentedAggregateExec;
use crate::physical_plan::unpivot::UnpivotExec;

fn build_groups(
    groups: Option<Vec<(Expr, SortField)>>,
    dfschema: &DFSchema,
    schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Option<(Vec<(PhysicalExprRef, String, arrow_row::SortField)>)>> {
    let ret = groups
        .clone()
        .map(|g| {
            g.iter()
                .map(|(expr, sf)| {
                    create_physical_expr(&expr, &dfschema, schema, &execution_props).and_then(
                        |physexpr| {
                            let sf = arrow_row::SortField::new(sf.data_type.clone());
                            Ok((
                                physexpr as PhysicalExprRef,
                                expr.display_name().unwrap(),
                                sf.to_owned(),
                            ))
                        },
                    )
                })
                .collect::<DFResult<Vec<_>>>()
                .and_then(|f| Ok(f))
        })
        .transpose()?;

    Ok(ret)
}

fn aggregate<T>(
    agg: &logical_plan::partitioned_aggregate::AggregateFunction,
) -> AggregateFunction<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display {
    match agg {
        logical_plan::partitioned_aggregate::AggregateFunction::Sum => AggregateFunction::new_sum(),
        logical_plan::partitioned_aggregate::AggregateFunction::Min => AggregateFunction::new_min(),
        logical_plan::partitioned_aggregate::AggregateFunction::Max => AggregateFunction::new_max(),
        logical_plan::partitioned_aggregate::AggregateFunction::Avg => AggregateFunction::new_avg(),
        logical_plan::partitioned_aggregate::AggregateFunction::Count => {
            AggregateFunction::new_count()
        }
    }
}

macro_rules! count {
    ($ty:ident,$filter:expr,$groups:expr,$predicate:expr,$partition_col:expr,$distinct:expr) => {
        Box::new(Count::<$ty>::try_new(
            $filter,
            $groups,
            $predicate,
            $partition_col,
            $distinct,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
}

macro_rules! aggregate {
    ($ty:ident,$out_ty:ident,$filter:expr,$groups:expr,$predicate:expr,$partition_col:expr,$agg:expr) => {
        Box::new(Aggregate::<$ty, $out_ty>::try_new(
            $filter,
            $groups,
            $partition_col,
            $predicate,
            $agg,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
}

macro_rules! partitioned_count {
    ($out_ty:ident,$filter:expr,$agg:expr,$groups:expr,$partition_col:expr,$distinct:expr) => {
        Box::new(PartitionedCount::<$out_ty>::try_new(
            $filter,
            $agg,
            $groups,
            $partition_col,
            $distinct,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
}

macro_rules! partitioned_aggregate {
    ($ty:ident,$inner_acc_ty:ident,$outer_acc_ty:ident,$filter:expr,$inner:expr,$outer:expr,$predicate:expr,$groups:expr,$partition_col:expr) => {
        Box::new(partitioned::aggregate2::Aggregate::<
            $ty,
            $inner_acc_ty,
            $outer_acc_ty,
        >::try_new(
            $filter,
            $inner,
            $outer,
            $predicate,
            $groups,
            $partition_col,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
}

enum ReturnType {
    Float64,
    Int64,
    Original,
    Casted,
}

fn get_return_type(
    dt: DataType,
    agg: &logical_plan::partitioned_aggregate::AggregateFunction,
) -> DataType {
    match agg {
        logical_plan::partitioned_aggregate::AggregateFunction::Avg => DataType::Float64,
        logical_plan::partitioned_aggregate::AggregateFunction::Count => DataType::Int64,
        logical_plan::partitioned_aggregate::AggregateFunction::Min
        | logical_plan::partitioned_aggregate::AggregateFunction::Max => dt,
        logical_plan::partitioned_aggregate::AggregateFunction::Sum => match dt {
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
            DataType::Int64 | DataType::UInt64 | DataType::Decimal128(_, _) => {
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
            }
            _ => unimplemented!("{dt:?}"),
        },
    }
}

pub fn build_partitioned_aggregate_expr(
    expr: AggregateExpr,
    schema: &Schema,
) -> Result<Box<dyn PartitionedAggregateExpr>> {
    let dfschema = schema.clone().to_dfschema()?;
    let execution_props = ExecutionProps::new();
    let ret = match expr {
        AggregateExpr::Count {
            filter,
            groups,
            predicate,
            partition_col,
            distinct,
        } => {
            let filter = build_filter(filter, &dfschema, schema, &execution_props)?;
            let groups = build_groups(groups, &dfschema, schema, &execution_props)?;
            let predicate = col(predicate, &dfschema);
            let partition_col = col(partition_col, &dfschema);

            let count = match predicate.data_type(schema)? {
                DataType::Int8 => count!(i8, filter, groups, predicate, partition_col, distinct),
                DataType::Int16 => count!(i16, filter, groups, predicate, partition_col, distinct),
                DataType::Int32 => count!(i32, filter, groups, predicate, partition_col, distinct),
                DataType::Int64 => count!(i64, filter, groups, predicate, partition_col, distinct),
                DataType::UInt8 => count!(u8, filter, groups, predicate, partition_col, distinct),
                DataType::UInt16 => count!(u16, filter, groups, predicate, partition_col, distinct),
                DataType::UInt32 => count!(u32, filter, groups, predicate, partition_col, distinct),
                DataType::UInt64 => count!(u64, filter, groups, predicate, partition_col, distinct),
                DataType::Float32 => {
                    count!(f32, filter, groups, predicate, partition_col, distinct)
                }
                DataType::Float64 => {
                    count!(f64, filter, groups, predicate, partition_col, distinct)
                }
                DataType::Decimal128(_, _) => {
                    count!(i128, filter, groups, predicate, partition_col, distinct)
                }
                _ => return Err(QueryError::Plan("unsupported predicate type".to_string())),
            };

            Ok(count)
        }
        AggregateExpr::Aggregate {
            filter,
            groups,
            partition_col,
            predicate,
            agg,
        } => {
            let dfschema = schema.clone().to_dfschema()?;
            let execution_props = ExecutionProps::new();
            let filter = build_filter(filter, &dfschema, schema, &execution_props)?;
            let groups = build_groups(groups, &dfschema, schema, &execution_props)?;
            let predicate = col(predicate, &dfschema);
            let partition_col = col(partition_col, &dfschema);

            let dt = predicate.data_type(schema)?;
            let expr = match predicate.data_type(schema)? {
                DataType::Int8 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(i8, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(i8, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int8 => {
                        let agg = aggregate::<i8>(&agg);
                        aggregate!(i8, i8, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },

                DataType::Int16 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(i16, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(i16, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int16 => {
                        let agg = aggregate::<i16>(&agg);
                        aggregate!(i16, i16, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::Int32 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(i32, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(i32, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int32 => {
                        let agg = aggregate::<i32>(&agg);
                        aggregate!(i32, i32, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::Int64 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(i64, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(i64, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Decimal128(_, _) => {
                        let agg = aggregate::<i128>(&agg);
                        aggregate!(i64, i128, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::UInt8 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(u8, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(u8, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt8 => {
                        let agg = aggregate::<u8>(&agg);
                        aggregate!(u8, u8, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt64 => {
                        let agg = aggregate::<u64>(&agg);
                        aggregate!(u8, u64, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::UInt16 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(u16, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(u16, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt16 => {
                        let agg = aggregate::<u16>(&agg);
                        aggregate!(u16, u16, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt64 => {
                        let agg = aggregate::<u64>(&agg);
                        aggregate!(u16, u64, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::UInt32 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(u32, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(u32, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt32 => {
                        let agg = aggregate::<u32>(&agg);
                        aggregate!(u32, u32, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt64 => {
                        let agg = aggregate::<u64>(&agg);
                        aggregate!(u32, u64, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::UInt64 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(u64, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(u64, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::UInt64 => {
                        let agg = aggregate::<u64>(&agg);
                        aggregate!(u64, u64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Decimal128(_, _) => {
                        let agg = aggregate::<i128>(&agg);
                        aggregate!(u64, i128, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::Float32 => match get_return_type(dt, &agg) {
                    DataType::Float32 => {
                        let agg = aggregate::<f32>(&agg);
                        aggregate!(f32, f32, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(f32, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(f32, i64, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::Float64 => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(f64, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(f64, i64, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                DataType::Decimal128(_, _) => match get_return_type(dt, &agg) {
                    DataType::Float64 => {
                        let agg = aggregate::<f64>(&agg);
                        aggregate!(i128, f64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Int64 => {
                        let agg = aggregate::<i64>(&agg);
                        aggregate!(i128, i64, filter, groups, predicate, partition_col, agg)
                    }
                    DataType::Decimal128(_, _) => {
                        let agg = aggregate::<i128>(&agg);
                        aggregate!(i128, i128, filter, groups, predicate, partition_col, agg)
                    }
                    _ => unreachable!(),
                },
                _ => return Err(QueryError::Plan("unsupported predicate type".to_string())),
            };

            Ok(expr)
        }
        AggregateExpr::PartitionedCount {
            filter,
            outer_fn,
            groups,
            partition_col,
            distinct,
        } => {
            let filter = build_filter(filter, &dfschema, schema, &execution_props)?;
            let groups = build_groups(groups, &dfschema, schema, &execution_props)?;
            let partition_col = col(partition_col, &dfschema);

            let expr = match get_return_type(DataType::Int64, &outer_fn) {
                DataType::Float64 => {
                    let agg = aggregate::<f64>(&outer_fn);
                    partitioned_count!(f64, filter, agg, groups, partition_col, distinct)
                }
                DataType::Int64 => {
                    let agg = aggregate::<i64>(&outer_fn);
                    partitioned_count!(i64, filter, agg, groups, partition_col, distinct)
                }
                DataType::Decimal128(_, _) => {
                    let agg = aggregate::<i128>(&outer_fn);
                    partitioned_count!(i128, filter, agg, groups, partition_col, distinct)
                }
                _ => unreachable!(),
            };

            Ok(expr)
        }
        AggregateExpr::PartitionedAggregate {
            filter,
            inner_fn,
            outer_fn,
            predicate,
            groups,
            partition_col,
        } => {
            let dfschema = schema.clone().to_dfschema()?;
            let execution_props = ExecutionProps::new();
            let filter = build_filter(filter, &dfschema, schema, &execution_props)?;
            let groups = build_groups(groups, &dfschema, schema, &execution_props)?;
            let predicate = col(predicate, &dfschema);
            let partition_col = col(partition_col, &dfschema);

            let ret = match predicate.data_type(schema)? {
                DataType::Int8 => {
                    let t1 = get_return_type(DataType::Int8, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Int8, &DataType::Int8) => {
                            let inner = aggregate::<i8>(&inner_fn);
                            let outer = aggregate::<i8>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i8,
                                i8,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int8, &DataType::Float64) => {
                            let inner = aggregate::<i8>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i8,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int8, &DataType::Int64) => {
                            let inner = aggregate::<i8>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i8,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i8,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Int16 => {
                    let t1 = get_return_type(DataType::Int16, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Int16, &DataType::Int16) => {
                            println!("111wae");
                            let inner = aggregate::<i16>(&inner_fn);
                            let outer = aggregate::<i16>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i16,
                                i16,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int16, &DataType::Float64) => {
                            let inner = aggregate::<i16>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i16,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int16, &DataType::Int64) => {
                            println!("222wae");
                            let inner = aggregate::<i16>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i16,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i16,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Int32 => {
                    let t1 = get_return_type(DataType::Int32, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Int32, &DataType::Int32) => {
                            let inner = aggregate::<i32>(&inner_fn);
                            let outer = aggregate::<i32>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i32,
                                i32,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int32, &DataType::Float64) => {
                            let inner = aggregate::<i32>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i32,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int32, &DataType::Int64) => {
                            let inner = aggregate::<i32>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i32,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i32,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Int64 => {
                    let t1 = get_return_type(DataType::Int64, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i128,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Float64) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i128,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Int64) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i64,
                                i128,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::UInt8 => {
                    let t1 = get_return_type(DataType::UInt8, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::UInt8, &DataType::UInt8) => {
                            let inner = aggregate::<u8>(&inner_fn);
                            let outer = aggregate::<u8>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u8,
                                u8,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt8, &DataType::Float64) => {
                            let inner = aggregate::<u8>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u8,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt8, &DataType::UInt64) => {
                            let inner = aggregate::<u8>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u8,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt8, &DataType::Int64) => {
                            let inner = aggregate::<u8>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u8,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::UInt64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u64,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Float64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Int64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                u64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u8,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::UInt16 => {
                    let t1 = get_return_type(DataType::UInt16, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::UInt16, &DataType::UInt16) => {
                            let inner = aggregate::<u16>(&inner_fn);
                            let outer = aggregate::<u16>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u16,
                                u16,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt16, &DataType::Float64) => {
                            let inner = aggregate::<u16>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u16,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt16, &DataType::UInt64) => {
                            let inner = aggregate::<u16>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u16,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt16, &DataType::Int64) => {
                            let inner = aggregate::<u16>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u16,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::UInt64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u64,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Float64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Int64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                u64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u16,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::UInt32 => {
                    let t1 = get_return_type(DataType::UInt32, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::UInt32, &DataType::UInt32) => {
                            let inner = aggregate::<u32>(&inner_fn);
                            let outer = aggregate::<u32>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u32,
                                u32,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt32, &DataType::Float64) => {
                            let inner = aggregate::<u32>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u32,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt32, &DataType::UInt64) => {
                            let inner = aggregate::<u32>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u32,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt32, &DataType::Int64) => {
                            let inner = aggregate::<u32>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u32,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::UInt64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u64,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Float64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Int64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                u64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u32,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::UInt64 => {
                    let t1 = get_return_type(DataType::UInt64, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::UInt64, &DataType::UInt64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<u64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u64,
                                u64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Float64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::UInt64, &DataType::Int64) => {
                            let inner = aggregate::<u64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                i128,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Float64) => {
                            let inner = aggregate::<u128>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u128,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                i128,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Int64) => {
                            let inner = aggregate::<u128>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                u128,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                u64,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Float32 => {
                    let t1 = get_return_type(DataType::Float32, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Float32, &DataType::Float32) => {
                            let inner = aggregate::<f32>(&inner_fn);
                            let outer = aggregate::<f32>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                f32,
                                f32,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float32, &DataType::Float64) => {
                            let inner = aggregate::<f32>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                f32,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float32, &DataType::Int64) => {
                            let inner = aggregate::<f32>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                f32,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                f32,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Float64 => {
                    let t1 = get_return_type(DataType::Float64, &inner_fn);
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                f64,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                f64,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                f64,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                f64,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                f64,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }

                DataType::Decimal128(_, _) => {
                    let t1 = get_return_type(
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        &inner_fn,
                    );
                    let t2 = get_return_type(t1.clone(), &outer_fn);
                    match (&t1, &t2) {
                        (&DataType::Decimal128(_, _), &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i128,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Float64) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i128,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Decimal128(_, _), &DataType::Int64) => {
                            let inner = aggregate::<i128>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i128,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Float64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                f64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Float64, &DataType::Int64) => {
                            let inner = aggregate::<f64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                f64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Int64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i64,
                                i64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Float64) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<f64>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i64,
                                f64,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        (&DataType::Int64, &DataType::Decimal128(_, _)) => {
                            let inner = aggregate::<i64>(&inner_fn);
                            let outer = aggregate::<i128>(&outer_fn);
                            partitioned_aggregate!(
                                i128,
                                i64,
                                i128,
                                filter,
                                inner,
                                outer,
                                predicate,
                                groups,
                                partition_col
                            )
                        }
                        _ => unimplemented!(),
                    }
                }
                _ => unimplemented!(),
            };

            Ok(ret)
        }
        AggregateExpr::Funnel {
            ts_col,
            from,
            to,
            window,
            steps,
            exclude,
            constants,
            count,
            filter,
            touch,
            partition_col,
            bucket_size,
            groups,
        } => {
            let steps = steps
                .iter()
                .map(|(expr, step_order)| {
                    create_physical_expr(&expr, &dfschema, schema, &execution_props).and_then(
                        |expr| {
                            let step_order = match step_order {
                                StepOrder::Sequential => partitioned::funnel::StepOrder::Sequential,
                                StepOrder::Any(v) => {
                                    partitioned::funnel::StepOrder::Any(v.to_owned())
                                }
                            };

                            Ok((expr, step_order))
                        },
                    )
                })
                .collect::<DFResult<Vec<_>>>()?;

            let exclude = exclude
                .clone()
                .map(|exprs| {
                    exprs
                        .iter()
                        .map(|expr| {
                            create_physical_expr(&expr.expr, &dfschema, schema, &execution_props)
                                .and_then(|pexpr| {
                                    let exclude_steps = expr.steps.clone().map(|es| {
                                        es.iter()
                                            .map(|es| partitioned::funnel::ExcludeSteps {
                                                from: es.from,
                                                to: es.to,
                                            })
                                            .collect::<Vec<_>>()
                                    });

                                    Ok(partitioned::funnel::ExcludeExpr {
                                        expr: pexpr,
                                        steps: None,
                                    })
                                })
                        })
                        .collect::<DFResult<Vec<_>>>()
                })
                .transpose()?;

            let constants = constants.clone().map(|cols| {
                cols.iter()
                    .map(|c| col(c.to_owned(), &dfschema))
                    .collect::<Vec<_>>()
            });
            let count = match count {
                logical_plan::partitioned_aggregate::funnel::Count::Unique => {
                    partitioned::funnel::Count::Unique
                }
                logical_plan::partitioned_aggregate::funnel::Count::NonUnique => {
                    partitioned::funnel::Count::NonUnique
                }
                logical_plan::partitioned_aggregate::funnel::Count::Session => {
                    partitioned::funnel::Count::Session
                }
            };

            let filter = filter.clone().map(|f| match f {
                Filter::DropOffOnAnyStep => partitioned::funnel::Filter::DropOffOnAnyStep,
                Filter::DropOffOnStep(s) => partitioned::funnel::Filter::DropOffOnStep(s),
                Filter::TimeToConvert(a, b) => partitioned::funnel::Filter::TimeToConvert(a, b),
            });

            let touch = match touch {
                Touch::First => partitioned::funnel::Touch::First,
                Touch::Last => partitioned::funnel::Touch::Last,
                Touch::Step(s) => partitioned::funnel::Touch::Step(s),
            };

            let groups = build_groups(groups, &dfschema, schema, &execution_props)?;
            let partition_col = col(partition_col, &dfschema);
            let opts = funnel::Options {
                schema: Arc::new(schema.clone()),
                ts_col: col(ts_col, &dfschema),
                from: from.to_owned(),
                to: to.to_owned(),
                window: window.to_owned(),
                steps,
                exclude,
                constants,
                count,
                filter,
                touch,
                partition_col,
                bucket_size,
                groups,
            };
            let funnel = Funnel::try_new(opts)?;
            Ok(Box::new(funnel) as Box<dyn PartitionedAggregateExpr>)
        }
    };

    ret
}
