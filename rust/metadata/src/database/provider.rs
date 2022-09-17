use std::sync::Arc;

use crate::database::types::{Column, Table, TableRef};
use bincode::serialize;

use tokio::sync::RwLock;

use crate::error::{DatabaseError, MetadataError};

use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"database";

pub struct Provider {
    store: Arc<Store>,
    tables: RwLock<Vec<Table>>,
}

impl Provider {
    pub fn new(store: Arc<Store>) -> Self {
        Provider {
            store,
            tables: RwLock::new(vec![]),
        }
    }

    pub async fn create_table(&self, table: Table) -> Result<()> {
        let mut tables = self.tables.write().await;
        if tables.iter().any(|t| t.typ == table.typ) {
            return Err(
                MetadataError::Database(DatabaseError::TableAlreadyExists(table.typ)).into(),
            );
        }

        tables.push(table.clone());

        self.persist(&table).await
    }

    pub async fn get_table(&self, table_type: TableRef) -> Result<Table> {
        let tables = self.tables.read().await;
        let table = tables.iter().find(|t| t.typ == table_type);

        match table {
            None => Err(MetadataError::Database(DatabaseError::TableNotFound(table_type)).into()),
            Some(table) => Ok(table.clone()),
        }
    }

    pub async fn add_column(&self, table_type: TableRef, col: Column) -> Result<()> {
        let mut tables = self.tables.write().await;
        let table = tables
            .iter_mut()
            .find(|t| t.typ == table_type)
            .ok_or_else(|| MetadataError::Database(DatabaseError::TableNotFound(table_type)))?;

        if table.columns.iter().any(|c| c.name == col.name) {
            return Err(MetadataError::Database(DatabaseError::ColumnAlreadyExists(col)).into());
        }

        table.columns.push(col.clone());

        self.persist(table).await
    }

    async fn persist(&self, table: &Table) -> Result<()> {
        let data = serialize(table)?;
        self.store.put(NAMESPACE, data).await
    }
}