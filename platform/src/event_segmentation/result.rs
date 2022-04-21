use datafusion::scalar::ScalarValue;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use crate::error::{Error, Result};

#[derive(Serialize, Deserialize, Debug)]
pub struct Series {
    dimension_headers: Vec<String>,
    metric_headers: Vec<String>,
    dimensions: Vec<Value>,
    series: Vec<Value>,
}

fn null_or_else<T, F: FnOnce(T) -> Value>(v: Option<T>, f: F) -> Value {
    v.map_or_else(|| Value::Null, |v| f(v))
}

fn try_convert_scalar_value(sv: &ScalarValue) -> Result<Value> {
    match sv {
        ScalarValue::Boolean(v) => Ok(null_or_else(v.clone(), |v| Value::from(v))),
        ScalarValue::Float32(v) => Ok(null_or_else(v.clone(), |v| Value::from(v))),
        ScalarValue::Float64(v) => Ok(null_or_else(v.clone(), |v| Value::from(v))),
        _ => unimplemented!(),
        /*ScalarValue::Decimal128(_, _, _) => {}
        ScalarValue::Int8(_) => {}
        ScalarValue::Int16(_) => {}
        ScalarValue::Int32(_) => {}
        ScalarValue::Int64(_) => {}
        ScalarValue::UInt8(_) => {}
        ScalarValue::UInt16(_) => {}
        ScalarValue::UInt32(_) => {}
        ScalarValue::UInt64(_) => {}
        ScalarValue::Utf8(_) => {}
        ScalarValue::LargeUtf8(_) => {}
        ScalarValue::Binary(_) => {}
        ScalarValue::LargeBinary(_) => {}
        ScalarValue::List(_, _) => {}
        ScalarValue::Date32(_) => {}
        ScalarValue::Date64(_) => {}
        ScalarValue::TimestampSecond(_, _) => {}
        ScalarValue::TimestampMillisecond(_, _) => {}
        ScalarValue::TimestampMicrosecond(_, _) => {}
        ScalarValue::TimestampNanosecond(_, _) => {}
        ScalarValue::IntervalYearMonth(_) => {}
        ScalarValue::IntervalDayTime(_) => {}
        ScalarValue::IntervalMonthDayNano(_) => {}
        ScalarValue::Struct(_, _) => {}*/
    }
}

impl TryFrom<query::reports::results::Series> for Series {
    type Error = Error;

    fn try_from(series: query::reports::results::Series) -> std::result::Result<Self, Self::Error> {
        Ok(Series {
            dimension_headers: series.dimension_headers.clone(),
            metric_headers: series.metric_headers.clone(),
            dimensions: series.dimensions.iter().map(|v| try_convert_scalar_value(v)).collect::<Result<_>>()?,
            series: series.series.iter().map(|v| try_convert_scalar_value(v)).collect::<Result<_>>()?,
        })
    }
}