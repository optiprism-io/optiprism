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
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::segmented_aggregate::funnel::ExcludeExpr;
use crate::logical_plan::segmented_aggregate::funnel::Filter;
use crate::logical_plan::segmented_aggregate::funnel::StepOrder;
use crate::logical_plan::segmented_aggregate::funnel::Touch;
use crate::logical_plan::segmented_aggregate::AggregateExpr;
use crate::logical_plan::segmented_aggregate::SegmentedAggregateNode;
use crate::logical_plan::segmented_aggregate::SortField;
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
use crate::physical_plan::segmented_aggregate::SegmentedAggregateExec;
use crate::physical_plan::unpivot::UnpivotExec;

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

fn build_filter(
    filter: Option<Expr>,
    dfschema: &DFSchema,
    schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let ret = filter
        .clone()
        .map(|e| create_physical_expr(&e, &dfschema, schema, &execution_props))
        .transpose()?;

    Ok(ret)
}

fn col(col: datafusion_common::Column, dfschema: &DFSchema) -> Column {
    Column::new(col.name.as_str(), dfschema.index_of_column(&col).unwrap())
}

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

fn aggregate(agg: &logical_plan::segmented_aggregate::AggregateFunction) -> AggregateFunction {
    match agg {
        logical_plan::segmented_aggregate::AggregateFunction::Sum => AggregateFunction::new_sum(),
        logical_plan::segmented_aggregate::AggregateFunction::Min => AggregateFunction::new_min(),
        logical_plan::segmented_aggregate::AggregateFunction::Max => AggregateFunction::new_max(),
        logical_plan::segmented_aggregate::AggregateFunction::Avg => AggregateFunction::new_avg(),
        logical_plan::segmented_aggregate::AggregateFunction::Count => {
            AggregateFunction::new_count()
        }
    }
}

fn segment_agg_expr(
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
                DataType::Int8 => Box::new(Count::<i8>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int16 => Box::new(Count::<i16>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int32 => Box::new(Count::<i32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int64 => Box::new(Count::<i64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt8 => Box::new(Count::<u8>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt16 => Box::new(Count::<u16>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt32 => Box::new(Count::<u32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt64 => Box::new(Count::<u64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float32 => Box::new(Count::<f32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float64 => Box::new(Count::<f64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Decimal128(_, _) => Box::new(Count::<i128>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    distinct,
                )?)
                    as Box<dyn PartitionedAggregateExpr>,
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
                DataType::Int8 => Box::new(Aggregate::<i8>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int16 => Box::new(Aggregate::<i16>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int32 => Box::new(Aggregate::<i32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int64 => Box::new(Aggregate::<i64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt8 => Box::new(Aggregate::<u8>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt16 => Box::new(Aggregate::<u16>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt32 => Box::new(Aggregate::<u32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt64 => Box::new(Aggregate::<u64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float32 => Box::new(Aggregate::<f32>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float64 => Box::new(Aggregate::<f64>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Decimal128(_, _) => Box::new(Aggregate::<i128>::try_new(
                    filter,
                    groups,
                    predicate,
                    partition_col,
                    agg,
                )?)
                    as Box<dyn PartitionedAggregateExpr>,
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
            let count =
                partitioned::count::Count::try_new(filter, outer, groups, partition_col, distinct)?;

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
                DataType::Int8 => Box::new(partitioned::aggregate::Aggregate::<i8>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int16 => Box::new(partitioned::aggregate::Aggregate::<i16>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int32 => Box::new(partitioned::aggregate::Aggregate::<i32>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Int64 => Box::new(partitioned::aggregate::Aggregate::<i64>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt8 => Box::new(partitioned::aggregate::Aggregate::<u8>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt16 => Box::new(partitioned::aggregate::Aggregate::<u16>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt32 => Box::new(partitioned::aggregate::Aggregate::<u32>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::UInt64 => Box::new(partitioned::aggregate::Aggregate::<u64>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float32 => Box::new(partitioned::aggregate::Aggregate::<f32>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Float64 => Box::new(partitioned::aggregate::Aggregate::<f64>::try_new(
                    filter,
                    inner,
                    outer,
                    predicate,
                    groups,
                    partition_col,
                )?) as Box<dyn PartitionedAggregateExpr>,
                DataType::Decimal128(_, _) => {
                    Box::new(partitioned::aggregate::Aggregate::<i128>::try_new(
                        filter,
                        inner,
                        outer,
                        predicate,
                        groups,
                        partition_col,
                    )?) as Box<dyn PartitionedAggregateExpr>
                }
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
                logical_plan::segmented_aggregate::funnel::Count::Unique => {
                    partitioned::funnel::Count::Unique
                }
                logical_plan::segmented_aggregate::funnel::Count::NonUnique => {
                    partitioned::funnel::Count::NonUnique
                }
                logical_plan::segmented_aggregate::funnel::Count::Session => {
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

#[async_trait]
impl DFExtensionPlanner for ExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
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
        } else if let Some(node) = any.downcast_ref::<SegmentedAggregateNode>() {
            let partition_inputs = match &node.partition_inputs {
                None => None,
                Some(c) => Some(physical_inputs[1..c.len()].to_vec()),
            };

            let partition_col = Column::new(
                node.partition_col.name.as_str(),
                node.schema.index_of_column(&node.partition_col).unwrap(),
            );

            let agg_expr = node
                .agg_expr
                .clone()
                .into_iter()
                .map(|(expr, name)| {
                    segment_agg_expr(expr, &physical_inputs[0].schema())
                        .and_then(|expr| Ok((Arc::new(Mutex::new(expr)), name)))
                })
                .collect::<Result<Vec<_>>>()
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            let exec = SegmentedAggregateExec::try_new(
                physical_inputs[0].clone(),
                partition_inputs,
                partition_col,
                agg_expr,
            )
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Some(Arc::new(exec) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}
