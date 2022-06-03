use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use datafusion::scalar::ScalarValue;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use crate::error::{Error, InternalError, Result};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Series {
    dimension_headers: Vec<String>,
    metric_headers: Vec<String>,
    dimensions: Vec<Vec<Value>>,
    series: Vec<Vec<Value>>,
}

fn null_or_else<T, F: FnOnce(T) -> Value>(v: Option<T>, f: F) -> Value {
    v.map_or_else(|| Value::Null, |v| f(v))
}

fn try_convert_scalar_value(sv: &ScalarValue) -> Result<Value> {
    match sv {
        ScalarValue::Boolean(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Float32(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Float64(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Utf8(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Int8(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Int16(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Int32(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Int64(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::UInt8(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::UInt16(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::UInt32(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::UInt64(v) => Ok(null_or_else(v.to_owned(), |v| Value::from(v))),
        ScalarValue::Decimal128(None, _, _) => Ok(Value::Null),
        ScalarValue::Decimal128(Some(i), p, s) => {
            let d = Decimal::try_new(*i as i64, *s as u32).map_err(|e| Error::Internal(e.to_string()))?;
            Ok(Value::from(d.to_f64().ok_or_else(|| Error::Internal("can't convert decimal to f64".to_string()))?))
        }
        _ => unimplemented!("{:?}", sv),
    }
}

impl TryFrom<query::reports::results::Series> for Series {
    type Error = Error;

    fn try_from(series: query::reports::results::Series) -> std::result::Result<Self, Self::Error> {
        Ok(Series {
            dimension_headers: series.dimension_headers.clone(),
            metric_headers: series.metric_headers.clone(),
            dimensions: series.dimensions
                .iter()
                .map(|v| v
                    .iter()
                    .map(|v| try_convert_scalar_value(v))
                    .collect())
                .collect::<Result<_>>()?,
            series: series.series
                .iter()
                .map(|v| v
                    .iter()
                    .map(|v| try_convert_scalar_value(v))
                    .collect())
                .collect::<Result<_>>()?,
        })
    }
}