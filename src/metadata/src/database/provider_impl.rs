use std::sync::Arc;

use async_trait::async_trait;
use bincode::serialize;
use tokio::sync::RwLock;

use crate::database::Column;
use crate::database::Provider;
use crate::database::Table;
use crate::database::TableRef;
use crate::error::DatabaseError;
use crate::error::MetadataError;
use crate::store::Store;
use crate::Result;
const NAMESPACE: &[u8] = b"database";

pub struct ProviderImpl {
    store: Arc<Store>,
    tables: RwLock<Vec<Table>>,
}

impl ProviderImpl {
    pub fn new(store: Arc<Store>) -> Self {
        ProviderImpl {
            store,
            tables: RwLock::new(vec![]),
        }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create_table(&self, table: Table) -> Result<()> {
        let mut tables = self.tables.write().await;
        if tables.iter().any(|t| t.typ == table.typ) {
            return Err(MetadataError::Database(DatabaseError::TableAlreadyExists(
                table.typ,
            )));
        }

        tables.push(table.clone());

        self.persist(&table).await
    }

    async fn get_table(&self, table_type: TableRef) -> Result<Table> {
        let tables = self.tables.read().await;
        let table = tables.iter().find(|t| t.typ == table_type);

        match table {
            None => Err(MetadataError::Database(DatabaseError::TableNotFound(
                table_type,
            ))),
            Some(table) => Ok(table.clone()),
        }
    }

    async fn add_column(&self, table_type: TableRef, col: Column) -> Result<()> {
        let mut tables = self.tables.write().await;
        let table =
            tables
                .iter_mut()
                .find(|t| t.typ == table_type)
                .ok_or(MetadataError::Database(DatabaseError::TableNotFound(
                    table_type,
                )))?;

        if table.columns.iter().any(|c| c.name == col.name) {
            return Err(MetadataError::Database(DatabaseError::ColumnAlreadyExists(
                col,
            )));
        }

        table.columns.push(col.clone());

        self.persist(table).await
    }

    async fn persist(&self, table: &Table) -> Result<()> {
        let data = serialize(table)?;
        self.store.put(NAMESPACE, data).await
    }
}
