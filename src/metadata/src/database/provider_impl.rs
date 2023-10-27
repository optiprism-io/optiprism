use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::database::Column;
use crate::database::CreateTableRequest;
use crate::database::Provider;
use crate::database::Table;
use crate::database::TableRef;
use crate::database::UpdateTableRequest;
use crate::error;
use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::metadata::ListResponse;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"database/tables";
const IDX_REF: &[u8] = b"ref";

fn index_keys(typ: &TableRef) -> Vec<Option<Vec<u8>>> {
    [index_ref_key(typ)].to_vec()
}

fn index_ref_key(typ: &TableRef) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_REF, typ.to_string().as_str()).to_vec())
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    pub fn _get_by_id(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Table> {
        let key = make_data_value_key(NAMESPACE, id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound("table not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn _get_by_ref(&self, tx: &Transaction<TransactionDB>, typ: &TableRef) -> Result<Table> {
        let data = get_index(
            &tx,
            make_index_key(NAMESPACE, IDX_REF, typ.to_string().as_str()).to_vec(),
        )?;
        Ok(deserialize(&data)?)
    }
}

impl Provider for ProviderImpl {
    fn create(&self, req: CreateTableRequest) -> Result<Table> {
        let tx = self.db.transaction();
        let idx_keys = index_keys(&req.typ);
        check_insert_constraints(&tx, idx_keys.as_ref())?;
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;
        let tbl = Table {
            id,
            typ: req.typ,
            columns: req.columns,
        };

        let data = serialize(&tbl)?;
        tx.put(make_data_value_key(NAMESPACE, tbl.id), &data)?;
        insert_index(&tx, idx_keys.as_ref(), &data)?;
        Ok(tbl)
    }

    fn get_by_id(&self, id: u64) -> Result<Table> {
        let tx = self.db.transaction();
        self._get_by_id(&tx, id)
    }

    fn get_by_ref(&self, typ: TableRef) -> Result<Table> {
        let tx = self.db.transaction();
        self._get_by_ref(&tx, &typ)
    }

    fn list(&self) -> Result<ListResponse<Table>> {
        let tx = self.db.transaction();
        list(&tx, NAMESPACE)
    }

    fn add_column(&self, typ: &TableRef, col: Column) -> Result<()> {
        let tx = self.db.transaction();
        let mut tbl = self._get_by_ref(&tx, typ)?;
        tbl.columns.push(col);
        let data = serialize(&tbl)?;
        tx.put(make_data_value_key(NAMESPACE, tbl.id), &data)?;

        Ok(())
    }

    fn update(&self, table_id: u64, req: UpdateTableRequest) -> Result<Table> {
        let tx = self.db.transaction();

        let prev_table = self._get_by_id(&tx, table_id)?;
        let mut table = prev_table.clone();

        table.typ = req.typ.clone();
        table.columns = req.columns.clone();
        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        idx_keys.push(index_ref_key(&req.typ));
        idx_prev_keys.push(index_ref_key(&prev_table.typ));
        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;
        let data = serialize(&table)?;
        tx.put(make_data_value_key(NAMESPACE, table.id), &data)?;
        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        Ok(table)
    }

    fn delete(&self, id: u64) -> Result<Table> {
        let tx = self.db.transaction();
        let tbl = self._get_by_id(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        delete_index(&tx, index_keys(&tbl.typ).as_ref())?;

        Ok(tbl)
    }
}
