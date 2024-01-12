use std::sync::Arc;



use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use super::CreateOrganizationRequest;
use super::Organization;

use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::metadata::ListResponse;
use crate::organizations::Provider;
use crate::organizations::UpdateOrganizationRequest;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::Result;

const NAMESPACE: &[u8] = b"organizations";
const IDX_NAME: &[u8] = b"name";

fn index_keys(name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(name)].to_vec()
}

fn index_name_key(name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_NAME, name).to_vec())
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    pub fn _get_by_id(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Organization> {
        let key = make_data_value_key(NAMESPACE, id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "organization not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(&self, req: CreateOrganizationRequest) -> Result<Organization> {
        let tx = self.db.transaction();

        let idx_keys = index_keys(&req.name);
        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let org = Organization {
            id,
            created_at,
            created_by: req.created_by,
            updated_at: None,
            updated_by: None,
            name: req.name,
        };

        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, id), &data)?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(org)
    }

    fn get_by_id(&self, id: u64) -> Result<Organization> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, id)
    }

    fn list(&self) -> Result<ListResponse<Organization>> {
        let tx = self.db.transaction();

        list(&tx, NAMESPACE)
    }

    fn update(&self, org_id: u64, req: UpdateOrganizationRequest) -> Result<Organization> {
        let tx = self.db.transaction();

        let prev_org = self._get_by_id(&tx, org_id)?;

        let mut org = prev_org.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(name.as_str()));
            idx_prev_keys.push(index_name_key(prev_org.name.as_str()));
            org.name = name.to_owned();
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        org.updated_at = Some(Utc::now());
        org.updated_by = Some(req.updated_by);

        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, org.id), &data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(org)
    }

    fn delete(&self, id: u64) -> Result<Organization> {
        let tx = self.db.transaction();

        let org = self._get_by_id(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        delete_index(&tx, index_keys(&org.name).as_ref())?;
        tx.commit()?;
        Ok(org)
    }
}
