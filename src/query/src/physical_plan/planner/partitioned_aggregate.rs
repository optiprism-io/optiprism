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
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel;
// use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::aggregate::AggregateFunction;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
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

fn aggregate(agg: &logical_plan::partitioned_aggregate::AggregateFunction) -> AggregateFunction {
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
    ($ty:ident,$filter:expr,$groups:expr,$predicate:expr,$partition_col:expr,$agg:expr) => {
        Box::new(Aggregate::<$ty>::try_new(
            $filter,
            $groups,
            $partition_col,
            $predicate,
            $agg,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
}

macro_rules! partitioned_aggregate {
    ($ty:ident,$filter:expr,$inner:expr,$outer:expr,$predicate:expr,$groups:expr,$partition_col:expr) => {
        Box::new(partitioned::aggregate::Aggregate::<$ty>::try_new(
            $filter,
            $inner,
            $outer,
            $predicate,
            $groups,
            $partition_col,
        )?) as Box<dyn PartitionedAggregateExpr>
    };
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
            let agg = aggregate(&agg);

            let count = match predicate.data_type(schema)? {
                DataType::Int8 => aggregate!(i8, filter, groups, predicate, partition_col, agg),
                DataType::Int16 => aggregate!(i16, filter, groups, predicate, partition_col, agg),
                DataType::Int32 => aggregate!(i32, filter, groups, predicate, partition_col, agg),
                DataType::Int64 => aggregate!(i64, filter, groups, predicate, partition_col, agg),
                DataType::UInt8 => aggregate!(u8, filter, groups, predicate, partition_col, agg),
                DataType::UInt16 => aggregate!(u16, filter, groups, predicate, partition_col, agg),
                DataType::UInt32 => aggregate!(u32, filter, groups, predicate, partition_col, agg),
                DataType::UInt64 => aggregate!(u64, filter, groups, predicate, partition_col, agg),
                DataType::Float32 => aggregate!(f32, filter, groups, predicate, partition_col, agg),
                DataType::Float64 => aggregate!(f64, filter, groups, predicate, partition_col, agg),
                DataType::Decimal128(_, _) => {
                    aggregate!(i128, filter, groups, predicate, partition_col, agg)
                }
                _ => return Err(QueryError::Plan("unsupported predicate type".to_string())),
            };

            Ok(count)
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
            let outer = aggregate(&outer_fn);
            let count = partitioned::count::PartitionedCount::try_new(
                filter,
                outer,
                groups,
                partition_col,
                distinct,
            )?;

            Ok(Box::new(count) as Box<dyn PartitionedAggregateExpr>)
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
            let inner = aggregate(&inner_fn);
            let outer = aggregate(&outer_fn);

            let ret = match predicate.data_type(schema)? {
                DataType::Int8 => partitioned_aggregate!(
                    i8,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Int16 => partitioned_aggregate!(
                    i16,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Int32 => partitioned_aggregate!(
                    i32,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Int64 => partitioned_aggregate!(
                    i64,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::UInt8 => partitioned_aggregate!(
                    u8,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::UInt16 => partitioned_aggregate!(
                    u16,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::UInt32 => partitioned_aggregate!(
                    u32,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::UInt64 => partitioned_aggregate!(
                    u64,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Float32 => partitioned_aggregate!(
                    f32,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Float64 => partitioned_aggregate!(
                    f64,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                DataType::Decimal128(_, _) => partitioned_aggregate!(
                    i128,
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col
                ),
                _ => return Err(QueryError::Plan("unsupported predicate type".to_string())),
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
