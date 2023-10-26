mod provider_impl;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use async_trait::async_trait;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::properties::DictionaryType;
use crate::Result;

pub trait Provider: Sync + Send {
    fn create_table(&self, table: Table) -> Result<()>;
    fn get_table(&self, table_type: TableRef) -> Result<Table>;
    fn add_column(&self, table_type: TableRef, col: Column) -> Result<()>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TableRef {
    Events(u64, u64),
    Users(u64, u64),
    System(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub typ: TableRef,
    pub columns: Vec<Column>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub dictionary: Option<DictionaryType>,
}

impl Column {
    pub fn new(
        name: String,
        data_type: DataType,
        nullable: bool,
        dictionary: Option<DictionaryType>,
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
                        match c.dictionary {
                            None => c.data_type.into(),
                            Some(v) => v.into(),
                        },
                        c.nullable,
                    )
                })
                .collect::<Vec<Field>>(),
        )
    }

    pub fn qualified_name(&self) -> String {
        self.typ.qualified_name()
    }
}

impl TableRef {
    pub fn qualified_name(&self) -> String {
        match &self {
            TableRef::Events(organization_id, project_id) => {
                format!("events({organization_id},{project_id})")
            }
            TableRef::Users(organization_id, project_id) => {
                format!("users({organization_id},{project_id})")
            }
            TableRef::System(name) => format!("system({name})"),
        }
    }
}
