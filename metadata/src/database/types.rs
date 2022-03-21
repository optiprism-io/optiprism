use arrow::datatypes::{DataType, Field, Schema};
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
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String, data_type: DataType, is_nullable: bool) -> Column {
        Column {
            name,
            data_type,
            nullable: is_nullable,
        }
    }
}

impl Table {
    pub fn arrow_schema(&self) -> Schema {
        Schema::new(
            self.columns
                .iter()
                .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
                .collect()
        )
    }
}