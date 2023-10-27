use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::accounts::Account;
use crate::accounts::CreateAccountRequest;
use crate::accounts::Provider;
use crate::accounts::UpdateAccountRequest;
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
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"accounts";
const IDX_EMAIL: &[u8] = b"email";

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    [index_email_key(email)].to_vec()
}

fn index_email_key(email: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_EMAIL, email).to_vec())
}

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    pub fn _get_by_id(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Account> {
        let key = make_data_value_key(NAMESPACE, id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound("account not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(&self, req: CreateAccountRequest) -> Result<Account> {
        let idx_keys = index_keys(&req.email);

        let tx = self.db.transaction();
        check_insert_constraints(&tx, idx_keys.as_ref())?;
        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let account = req.into_account(id, created_at);

        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(account)
    }

    fn get_by_id(&self, id: u64) -> Result<Account> {
        let tx = self.db.transaction();
        self._get_by_id(&tx, id)
    }

    fn get_by_email(&self, email: &str) -> Result<Account> {
        let tx = self.db.transaction();
        let data = get_index(&tx, make_index_key(NAMESPACE, IDX_EMAIL, email))?;
        Ok(deserialize(&data)?)
    }

    fn list(&self) -> Result<ListResponse<Account>> {
        let tx = self.db.transaction();
        list(&tx, NAMESPACE)
    }

    fn update(&self, account_id: u64, req: UpdateAccountRequest) -> Result<Account> {
        let tx = self.db.transaction();

        let prev_account = self._get_by_id(&tx, account_id)?;
        let mut account = prev_account.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(email) = &req.email {
            idx_keys.push(index_email_key(email.as_str()));
            idx_prev_keys.push(index_email_key(prev_account.email.as_str()));
            account.email = email.to_owned();
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        account.updated_at = Some(Utc::now());
        account.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(first_name) = req.first_name {
            account.first_name = first_name;
        }
        if let OptionalProperty::Some(last_name) = req.last_name {
            account.last_name = last_name;
        }
        if let OptionalProperty::Some(organizations) = req.organizations {
            account.organizations = organizations;
        }
        if let OptionalProperty::Some(projects) = req.projects {
            account.projects = projects;
        }
        if let OptionalProperty::Some(teams) = req.teams {
            account.teams = teams;
        }

        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(account)
    }

    fn delete(&self, id: u64) -> Result<Account> {
        let tx = self.db.transaction();
        let account = self._get_by_id(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        delete_index(&tx, index_keys(&account.email).as_ref())?;
        tx.commit()?;
        Ok(account)
    }
}
