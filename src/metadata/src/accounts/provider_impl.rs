use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;

use crate::accounts::Account;
use crate::accounts::CreateAccountRequest;
use crate::accounts::Provider;
use crate::accounts::UpdateAccountRequest;
use crate::error;
use crate::error::AccountError;
use crate::error::MetadataError;
use crate::error::StoreError;
use crate::metadata::ListResponse;
use crate::store::index::hash_map::HashMap;
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
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>) -> Self {
        ProviderImpl {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
        }
    }

    fn _create(&self, req: CreateAccountRequest) -> Result<Account> {
        let idx_keys = index_keys(&req.email);

        match self.idx.check_insert_constraints(idx_keys.as_ref()) {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(
                    AccountError::AccountAlreadyExist(error::Account::new_with_email(req.email))
                        .into(),
                );
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        let created_at = Utc::now();
        let id = self.store.next_seq(make_id_seq_key(NAMESPACE))?;

        let account = req.into_account(id, created_at);

        let data = serialize(&account)?;
        self.store
            .put(make_data_value_key(NAMESPACE, account.id), &data)?;

        self.idx.insert(idx_keys.as_ref(), &data)?;

        Ok(account)
    }

    fn _get_by_email(&self, email: &str) -> Result<Account> {
        match self.idx.get(make_index_key(NAMESPACE, IDX_EMAIL, email)) {
            Err(MetadataError::Store(StoreError::KeyNotFound(_))) => Err(
                AccountError::AccountNotFound(error::Account::new_with_email(email.to_string()))
                    .into(),
            ),
            Err(other) => Err(other),
            Ok(data) => Ok(deserialize(&data)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(&self, req: CreateAccountRequest) -> Result<Account> {
        let _guard = self.guard.write().unwrap();
        self._create(req)
    }

    fn get_by_id(&self, id: u64) -> Result<Account> {
        let key = make_data_value_key(NAMESPACE, id);

        match self.store.get(key)? {
            None => Err(AccountError::AccountNotFound(error::Account::new_with_id(id)).into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    fn get_by_email(&self, email: &str) -> Result<Account> {
        let _guard = self.guard.read();
        self._get_by_email(email)
    }

    fn list(&self) -> Result<ListResponse<Account>> {
        list(self.store.clone(), NAMESPACE)
    }

    fn update(&self, account_id: u64, req: UpdateAccountRequest) -> Result<Account> {
        let _guard = self.guard.write().unwrap();

        let prev_account = self.get_by_id(account_id)?;

        let mut account = prev_account.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(email) = &req.email {
            idx_keys.push(index_email_key(email.as_str()));
            idx_prev_keys.push(index_email_key(prev_account.email.as_str()));
            account.email = email.to_owned();
        }

        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(
                    AccountError::AccountAlreadyExist(error::Account::new_with_id(account_id))
                        .into(),
                );
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

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
        self.store
            .put(make_data_value_key(NAMESPACE, account.id), &data)?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;

        Ok(account)
    }

    fn delete(&self, id: u64) -> Result<Account> {
        let _guard = self.guard.write().unwrap();
        let account = self.get_by_id(id)?;
        self.store.delete(make_data_value_key(NAMESPACE, id))?;

        self.idx.delete(index_keys(&account.email).as_ref())?;

        Ok(account)
    }
}
