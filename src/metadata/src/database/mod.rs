mod provider_impl;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use async_trait::async_trait;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::properties::DictionaryType;
use crate::Result;

pub trait Provider: Sync + Send {
    fn create(&self, req: CreateTableRequest) -> Result<Table>;
    fn get_by_id(&self, id: u64) -> Result<Table>;
    fn get_by_ref(&self, typ: TableRef) -> Result<Table>;
    fn list(&self) -> Result<ListResponse<Table>>;
    fn add_column(&self, typ: &TableRef, col: Column) -> Result<()>;
    fn update(&self, table_id: u64, req: UpdateTableRequest) -> Result<Table>;
    fn delete(&self, id: u64) -> Result<Table>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TableRef {
    Events(u64, u64),
    Users(u64, u64),
    System(String),
}

impl ToString for TableRef {
    fn to_string(&self) -> String {
        match self {
            TableRef::Events(org, proj) => format!("events_{}_{}", org, proj),
            TableRef::Users(org, proj) => format!("users_{}_{}", org, proj),
            TableRef::System(name) => format!("system_{}", name),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub id: u64,
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CreateTableRequest {
    pub typ: TableRef,
    pub columns: Vec<Column>,
}

pub struct UpdateTableRequest {
    pub typ: TableRef,
    pub columns: Vec<Column>,
}
