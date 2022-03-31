use crate::common::{PropValueOperation, PropertyRef, QueryTime};
use crate::{event_fields, Context, Error, Result};
use datafusion::error::Result as DFResult;
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::expr_fn::{and, binary_expr, or};
use datafusion_expr::{AccumulatorFunctionImplementation, AggregateFunction, AggregateUDF, col, Expr, Operator, ReturnTypeFunction, Signature, StateTypeFunction, Volatility};
pub use datafusion_expr::{lit, lit_timestamp_nano, Literal};
use metadata::properties::provider::Namespace;
use metadata::Metadata;
use std::sync::Arc;
use arrow::datatypes::DataType;
use datafusion::physical_plan::aggregates::return_type;
use crate::physical_plan::expressions::aggregate::state_types;
use crate::physical_plan::expressions::partitioned_aggregate::{PartitionedAggregate, PartitionedAggregateFunction};
use crate::physical_plan::expressions::sorted_distinct_count::SortedDistinctCount;
use datafusion::logical_plan::ExprSchemable;

pub fn multi_or(exprs: Vec<Expr>) -> Expr {
    // combine multiple values with OR
    // create initial OR between two first expressions
    let mut expr = or(exprs[0].clone(), exprs[1].clone());
    // iterate over rest of expression (3rd and so on) and add them to the final expression
    for vexpr in exprs.iter().skip(2) {
        // wrap into OR
        expr = or(expr.clone(), vexpr.clone());
    }

    expr
}

pub fn multi_and(exprs: Vec<Expr>) -> Expr {
    let mut expr = and(exprs[0].clone(), exprs[1].clone());
    for fexpr in exprs.iter().skip(2) {
        expr = and(expr.clone(), fexpr.clone())
    }

    expr
}

pub fn lit_timestamp(ts: i64) -> Expr {
    lit(ScalarValue::TimestampMicrosecond(Some(ts), None))
}

/// builds expression on timestamp
pub fn time_expression(time: &QueryTime) -> Expr {
    let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
    match time {
        QueryTime::Between { from, to } => {
            let left = binary_expr(
                ts_col.clone(),
                Operator::GtEq,
                lit_timestamp(from.timestamp_nanos() / 1_000),
            );

            let right = binary_expr(
                ts_col,
                Operator::LtEq,
                lit_timestamp(to.timestamp_nanos() / 1_000),
            );

            and(left, right)
        }
        QueryTime::From(from) => binary_expr(
            ts_col,
            Operator::GtEq,
            lit_timestamp(from.timestamp_nanos() / 1_000),
        ),
        QueryTime::Last { n: last, unit } => {
            let from = unit.sub(*last);
            binary_expr(ts_col, Operator::GtEq, lit_timestamp(from.timestamp()))
        }
    }
}

/// builds name [property] [op] [value] expression
pub async fn property_expression(
    ctx: &Context,
    md: &Arc<Metadata>,
    property: &PropertyRef,
    operation: &PropValueOperation,
    value: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match property {
        PropertyRef::User(_) | PropertyRef::Event(_) => {
            let prop_col = property_col(ctx, md, &property).await?;
            named_property_expression(prop_col, operation, value)
        }
        PropertyRef::UserCustom(_) => unimplemented!(),
        PropertyRef::EventCustom(_) => unimplemented!(),
    }
}

pub fn sorted_distinct_count(input_schema: &DFSchema, col: Expr) -> Result<Expr> {
    let name = "sorted_distinct_count".to_string();
    let data_type = col.get_type(input_schema)?;
    let sorted_distinct = SortedDistinctCount::new(name, data_type);
    let udf = sorted_distinct.try_into()?;

    Ok(Expr::AggregateUDF {
        fun: Arc::new(udf),
        args: vec![col],
    })
}

pub fn aggregate_partitioned(
    input_schema: &DFSchema,
    partition_by: Expr,
    fun: &PartitionedAggregateFunction,
    outer_fun: &AggregateFunction,
    args: Vec<Expr>,
) -> Result<Expr> {
    // determine arguments data types
    let data_types = args
        .iter()
        .map(|x| x.get_type(input_schema))
        .collect::<DFResult<Vec<DataType>>>()?;
    // determine return type
    let rtype = return_type(&outer_fun, &data_types)?;

    // determine state types
    let state_types: Vec<DataType> = state_types(rtype.clone(), &outer_fun)?;

    // make partitioned aggregate factory
    let pagg = PartitionedAggregate::try_new(
        partition_by.get_type(input_schema)?,
        rtype.clone(),
        fun.clone(),
        rtype.clone(),
        outer_fun.clone(),
    )?;

    // factory closure
    let acc_fn: AccumulatorFunctionImplementation = Arc::new(move || {
        pagg.create_accumulator()
            .map_err(Error::into_datafusion_plan_error)
    });
    let return_type_fn: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(rtype.clone())));
    let state_type_fn: StateTypeFunction = Arc::new(move |_| Ok(Arc::new(state_types.clone())));

    let udf = AggregateUDF::new(
        "AggregatePartitioned", // TODO make name based on aggregates
        &Signature::any(2, Volatility::Immutable),
        &return_type_fn,
        &acc_fn,
        &state_type_fn,
    );

    Ok(Expr::AggregateUDF {
        fun: Arc::new(udf),
        // join partition and function arguments into one vector
        args: vec![vec![partition_by.clone()], args].concat(),
    })
}

pub async fn property_col(
    ctx: &Context,
    md: &Arc<Metadata>,
    property: &PropertyRef,
) -> Result<Expr> {
    Ok(match property {
        PropertyRef::User(prop_name) => {
            let prop = md
                .user_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            col(prop.column_name(Namespace::User).as_str())
        }
        PropertyRef::UserCustom(_prop_name) => unimplemented!(),
        PropertyRef::Event(prop_name) => {
            let prop = md
                .event_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            col(prop.column_name(Namespace::Event).as_str())
        }
        PropertyRef::EventCustom(_) => unimplemented!(),
    })
}

/// builds "[property] [op] [values]" binary expression with already known property column
pub fn named_property_expression(
    prop_col: Expr,
    operation: &PropValueOperation,
    values: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match operation {
        PropValueOperation::Eq | PropValueOperation::Neq => {
            // expressions for OR
            let mut exprs: Vec<Expr> = vec![];

            let values_vec = values.ok_or_else(|| {
                Error::QueryError("value should be defined for this kind of operation".to_owned())
            })?;

            Ok(match values_vec.len() {
                1 => binary_expr(
                    prop_col.clone(),
                    operation.clone().into(),
                    lit(values_vec[0].clone()),
                ),
                _ => {
                    // iterate over all possible values
                    let exprs = values_vec
                        .iter()
                        .map(|v| {
                            binary_expr(prop_col.clone(), operation.clone().into(), lit(v.clone()))
                        })
                        .collect();

                    multi_or(exprs)
                }
            })
        }
        // for isNull and isNotNull we don't need values at all
        PropValueOperation::IsNull => Ok(prop_col.is_null()),
        PropValueOperation::IsNotNull => Ok(prop_col.is_not_null()),
    }
}
