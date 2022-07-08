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
use crate::{arr_to_json_values, array_ref_to_json_values};

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

impl TryFrom<query::data_table::DataTable> for DataTable {
    type Error = Error;

    fn try_from(
        value: query::data_table::DataTable,
    ) -> std::result::Result<Self, Self::Error> {
        let cols = value
            .columns
            .iter()
            .map(|column| {
                match array_ref_to_json_values(column.data()) {
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
