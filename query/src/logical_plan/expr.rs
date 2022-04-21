use std::ops::Sub;
use crate::reports::types::{PropValueOperation, PropertyRef, QueryTime};
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
use chrono::{DateTime, Utc};
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
    fun: PartitionedAggregateFunction,
    outer_fun: AggregateFunction,
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
        fun,
        rtype.clone(),
        outer_fun,
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