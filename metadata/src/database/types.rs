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
    pub dictionary: Option<DataType>,
}

impl Column {
    pub fn new(
        name: String,
        data_type: DataType,
        nullable: bool,
        dictionary: Option<DataType>,
    ) -> Column {
        Column {
            name,
            data_type,
            nullable,
            dictionary,
        }
    }
}

impl Table {
    pub fn arrow_schema(self) -> Schema {
        Schema::new(
            self.columns
                .iter()
                .cloned()
                .map(|c| {
                    Field::new(
                        &c.name,
                        c.dictionary.unwrap_or_else(|| c.data_type.clone()),
                        c.nullable,
                    )
                })
                .collect(),
        )
    }
}
