use crate::error::{Error, Result};
use arrow::array::{
    Array, BooleanArray, DecimalArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Number, Value};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    pub group: String,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub data: Vec<Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataTable {
    columns: Vec<Column>,
}

impl DataTable {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}
macro_rules! arr_to_json_values {
    ($array_ref:expr,$array_type:ident) => {{
        let arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        Ok(arr.iter().map(|value| json!(value)).collect())
    }};
}
impl TryFrom<query::data_table::DataTable> for DataTable {
    type Error = Error;

    fn try_from(
        value: query::data_table::DataTable,
    ) -> std::result::Result<Self, Self::Error> {
        let cols = value
            .columns
            .iter()
            .map(|column| {
                let data = match column.data_type() {
                    DataType::Int8 => arr_to_json_values!(column.data(), Int8Array),
                    DataType::Int16 => arr_to_json_values!(column.data(), Int16Array),
                    DataType::Int32 => arr_to_json_values!(column.data(), Int32Array),
                    DataType::Int64 => arr_to_json_values!(column.data(), Int64Array),
                    DataType::UInt8 => arr_to_json_values!(column.data(), UInt8Array),
                    DataType::UInt16 => arr_to_json_values!(column.data(), UInt16Array),
                    DataType::UInt32 => arr_to_json_values!(column.data(), UInt32Array),
                    DataType::UInt64 => arr_to_json_values!(column.data(), UInt64Array),
                    DataType::Float32 => arr_to_json_values!(column.data(), Float32Array),
                    DataType::Float64 => arr_to_json_values!(column.data(), Float64Array),
                    DataType::Boolean => arr_to_json_values!(column.data(), BooleanArray),
                    DataType::Utf8 => arr_to_json_values!(column.data(), StringArray),
                    DataType::Decimal(_, s) => {
                        let arr = column
                            .data()
                            .as_any()
                            .downcast_ref::<DecimalArray>()
                            .unwrap();
                        arr.iter()
                            .map(|value| match value {
                                None => Ok(Value::Null),
                                Some(v) => {
                                    let d = match Decimal::try_new(v as i64, *s as u32) {
                                        Ok(v) => v,
                                        Err(err) => return Err(err.into()),
                                    };
                                    let d_f = match d.to_f64() {
                                        None => {
                                            return Err(Error::Internal(
                                                "can't convert decimal to f64".to_string(),
                                            ))
                                        }
                                        Some(v) => v,
                                    };
                                    let n = match Number::from_f64(d_f) {
                                        None => {
                                            return Err(Error::Internal(
                                                "can't make json number from f64".to_string(),
                                            ))
                                        }
                                        Some(v) => v,
                                    };
                                    Ok(Value::Number(n))
                                }
                            })
                            .collect::<Result<_>>()
                    }
                    _ => unimplemented!(),
                };

                match data {
                    Ok(data) => Ok(Column {
                        name: column.name().to_string(),
                        group: column.group().to_string(),
                        is_nullable: column.is_nullable(),
                        data_type: column.data_type().to_owned(),
                        data,
                    }),
                    Err(err) => Err(err),
                }
            })
            .collect::<Result<_>>()?;

        Ok(DataTable::new(cols))
    }
}
