use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum TableType {
    Events(u64, u64),
    Users(u64, u64),
    System(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Table {
    pub typ: TableType,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub is_nullable: bool,
}