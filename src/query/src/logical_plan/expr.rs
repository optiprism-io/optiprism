

use arrow::datatypes::DataType;
use chrono::DateTime;
use chrono::Utc;



use datafusion_common::ScalarValue;


use datafusion_expr::expr_fn::and;
use datafusion_expr::expr_fn::or;
pub use datafusion_expr::lit;
pub use datafusion_expr::lit_timestamp_nano;



use datafusion_expr::Expr;

pub use datafusion_expr::Literal;





use crate::error::QueryError;
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
                "unsupported \"{data_type:?}\" timestamp data type"
            )));
        }
    }))
}
