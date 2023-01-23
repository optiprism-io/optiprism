use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::DateTime;
use chrono::Utc;
use common::query::PartitionedAggregateFunction;
use datafusion_common::DFSchema;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_expr::aggregate_function::return_type;
use datafusion_expr::expr_fn::and;
use datafusion_expr::expr_fn::or;
pub use datafusion_expr::lit;
pub use datafusion_expr::lit_timestamp_nano;
use datafusion_expr::AccumulatorFunctionImplementation;
use datafusion_expr::AggregateFunction;
use datafusion_expr::AggregateUDF;
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
pub use datafusion_expr::Literal;
use datafusion_expr::ReturnTypeFunction;
use datafusion_expr::Signature;
use datafusion_expr::StateTypeFunction;
use datafusion_expr::Volatility;

use crate::error::QueryError;
use crate::physical_plan::expressions::aggregate::state_types;
use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAggregate;
use crate::physical_plan::expressions::sorted_distinct_count::SortedDistinctCount;
use crate::Result;

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

pub fn lit_timestamp(data_type: DataType, date_time: &DateTime<Utc>) -> Result<Expr> {
    Ok(lit(match data_type {
        DataType::Timestamp(arrow::datatypes::TimeUnit::Second, tz) => {
            ScalarValue::TimestampSecond(Some(date_time.timestamp()), tz)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, tz) => {
            ScalarValue::TimestampMillisecond(Some(date_time.timestamp_millis()), tz)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(date_time.timestamp_nanos() / 1000), tz)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, tz) => {
            ScalarValue::TimestampNanosecond(Some(date_time.timestamp_nanos()), tz)
        }
        _ => {
            return Err(QueryError::Plan(format!(
                "unsupported \"{:?}\" timestamp data type",
                data_type
            )));
        }
    }))
}

pub fn sorted_distinct_count(input_schema: &DFSchema, col: Expr) -> Result<Expr> {
    let name = "sorted_distinct_count".to_string();
    let data_type = col.get_type(input_schema)?;
    let sorted_distinct = SortedDistinctCount::new(name, data_type);
    let udf = sorted_distinct.try_into()?;

    Ok(Expr::AggregateUDF {
        fun: Arc::new(udf),
        args: vec![col],
        filter: None,
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
        args[0].get_type(input_schema)?,
        fun,
        rtype.clone(),
        outer_fun,
    )?;

    // factory closure
    let acc_fn: AccumulatorFunctionImplementation = Arc::new(move |_| {
        pagg.create_accumulator()
            .map_err(QueryError::into_datafusion_plan_error)
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
        args: vec![vec![partition_by], args].concat(),
        filter: None,
    })
}
