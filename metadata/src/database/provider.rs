use arrow::datatypes::DataType;
use std::sync::Arc;

use crate::database::types::{Column, Table, TableType};
use bincode::{deserialize, serialize};
use chrono::Utc;
use datafusion::parquet::data_type::AsBytes;
use tokio::sync::RwLock;

use crate::error::Error;
use crate::events::types::{CreateEventRequest, UpdateEventRequest};
use crate::events::Event;
use crate::metadata::{list, ListResponse};
use crate::store::index::hash_map::HashMap;
use crate::store::store::{make_data_value_key, make_id_seq_key, make_index_key, Store};
use crate::Result;

const NAMESPACE: &[u8] = b"database";

pub struct Provider {
    store: Arc<Store>,
    tables: RwLock<Vec<Table>>,
}

impl Provider {
    pub fn new(store: Arc<Store>) -> Self {
        Provider {
            store: store.clone(),
            tables: RwLock::new(vec![]),
        }
    }

    pub async fn create_table(&self, table: Table) -> Result<()> {
        let mut tables = self.tables.write().await;
        if tables.iter().find(|t| t.typ == table.typ).is_some() {
            return Err(Error::KeyAlreadyExists);
        }

        tables.push(table.clone());

        self.persist(&table).await
    }

    pub async fn get_table(&self, table_type: TableType) -> Result<Table> {
        let tables = self.tables.read().await;
        let table = tables.iter().find(|t| t.typ == table_type);

        match table {
            None => Err(Error::KeyNotFound("table".to_string())),
            Some(table) => Ok(table.clone()),
        }
    }

    pub async fn add_column(&self, table_type: TableType, col: Column) -> Result<()> {
        let mut tables = self.tables.write().await;
        let table = tables
            .iter_mut()
            .find(|t| t.typ == table_type)
            .ok_or_else(|| Error::KeyNotFound("table".to_string()))?;

        if table.columns.iter().find(|c| c.name == col.name).is_some() {
            return Err(Error::KeyAlreadyExists);
        }

        table.columns.push(col.clone());

        self.persist(&table).await
    }

    async fn persist(&self, table: &Table) -> Result<()> {
        let data = serialize(table)?;
        self.store.put(NAMESPACE, data).await
    }
}
