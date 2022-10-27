pub mod accounts;
pub mod auth;
pub mod context;
pub mod custom_events;
pub mod data_table;
pub mod error;
pub mod events;
pub mod http;
pub mod properties;
pub mod provider;
pub mod queries;
pub mod types;
pub mod validator;

pub use accounts::Provider as AccountsProvider;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::StringArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
pub use auth::Provider as AuthProvider;
pub use context::Context;
pub use custom_events::Provider as CustomEventsProvider;
pub use error::PlatformError;
pub use error::Result;
pub use events::Provider as EventsProvider;
pub use properties::Provider as PropertiesProvider;
pub use provider::PlatformProvider;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::json;
use serde_json::Number;
use serde_json::Value;

#[macro_export]
macro_rules! arr_to_json_values {
    ($array_ref:expr,$array_type:ident) => {{
        let arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        Ok(arr.iter().map(|value| json!(value)).collect())
    }};
}

pub fn array_ref_to_json_values(arr: &ArrayRef) -> Result<Vec<Value>> {
    match arr.data_type() {
        DataType::Int8 => arr_to_json_values!(arr, Int8Array),
        DataType::Int16 => arr_to_json_values!(arr, Int16Array),
        DataType::Int32 => arr_to_json_values!(arr, Int32Array),
        DataType::Int64 => arr_to_json_values!(arr, Int64Array),
        DataType::UInt8 => arr_to_json_values!(arr, UInt8Array),
        DataType::UInt16 => arr_to_json_values!(arr, UInt16Array),
        DataType::UInt32 => arr_to_json_values!(arr, UInt32Array),
        DataType::UInt64 => arr_to_json_values!(arr, UInt64Array),
        DataType::Float32 => arr_to_json_values!(arr, Float32Array),
        DataType::Float64 => arr_to_json_values!(arr, Float64Array),
        DataType::Boolean => arr_to_json_values!(arr, BooleanArray),
        DataType::Utf8 => arr_to_json_values!(arr, StringArray),
        DataType::Decimal128(_, s) => {
            let arr = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
            arr.iter()
                .map(|value| match value {
                    None => Ok(Value::Null),
                    Some(v) => {
                        let d = match Decimal::try_new(v.as_i128() as i64, *s as u32) {
                            Ok(v) => v,
                            Err(err) => return Err(err.into()),
                        };
                        let d_f = match d.to_f64() {
                            None => {
                                return Err(PlatformError::Internal(
                                    "can't convert decimal to f64".to_string(),
                                ));
                            }
                            Some(v) => v,
                        };
                        let n = match Number::from_f64(d_f) {
                            None => {
                                return Err(PlatformError::Internal(
                                    "can't make json number from f64".to_string(),
                                ));
                            }
                            Some(v) => v,
                        };
                        Ok(Value::Number(n))
                    }
                })
                .collect::<Result<_>>()
        }
        _ => unimplemented!(),
    }
}
