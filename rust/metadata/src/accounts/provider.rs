use std::sync::Arc;

use bincode::{deserialize, serialize};
use chrono::Utc;

use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::accounts::types::UpdateAccountRequest;
use crate::accounts::{Account, CreateAccountRequest};
use crate::error::{AccountError, MetadataError, StoreError};
use crate::metadata::ListResponse;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::{list, make_data_value_key, make_id_seq_key, make_index_key};
use crate::store::Store;
use crate::{error, Result};

const NAMESPACE: &[u8] = b"accounts";
const IDX_EMAIL: &[u8] = b"email";

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    [index_email_key(email)].to_vec()
}

fn index_email_key(email: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_EMAIL, email).to_vec())
}

pub struct Provider {
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
        }
    }

    pub async fn create(&self, req: CreateAccountRequest) -> Result<Account> {
        let _guard = self.guard.write().await;
        self._create(req).await
    }

    pub async fn _create(&self, req: CreateAccountRequest) -> Result<Account> {
        let idx_keys = index_keys(&req.email);

        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
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
        let id = self.store.next_seq(make_id_seq_key(NAMESPACE)).await?;

        let account = req.into_account(id, created_at);

        let data = serialize(&account)?;
        self.store
            .put(make_data_value_key(NAMESPACE, account.id), &data)
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;

        Ok(account)
    }

    pub async fn get_by_id(&self, id: u64) -> Result<Account> {
        let key = make_data_value_key(NAMESPACE, id);

        match self.store.get(key).await? {
            None => Err(AccountError::AccountNotFound(error::Account::new_with_id(id)).into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_email(&self, email: &str) -> Result<Account> {
        let _guard = self.guard.read().await;
        self._get_by_email(email).await
    }

    pub async fn _get_by_email(&self, email: &str) -> Result<Account> {
        match self
            .idx
            .get(make_index_key(NAMESPACE, IDX_EMAIL, email))
            .await
        {
            Err(MetadataError::Store(StoreError::KeyNotFound(_))) => Err(
                AccountError::AccountNotFound(error::Account::new_with_email(email.to_string()))
                    .into(),
            ),
            Err(other) => Err(other),
            Ok(data) => Ok(deserialize(&data)?),
        }
    }

    pub async fn list(&self) -> Result<ListResponse<Account>> {
        list(self.store.clone(), NAMESPACE).await
    }

    pub async fn update(&self, account_id: u64, req: UpdateAccountRequest) -> Result<Account> {
        let _guard = self.guard.write().await;

        let prev_account = self.get_by_id(account_id).await?;

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
            .await
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
            .put(make_data_value_key(NAMESPACE, account.id), &data)
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;

        Ok(account)
    }

    pub async fn delete(&self, id: u64) -> Result<Account> {
        let _guard = self.guard.write().await;
        let account = self.get_by_id(id).await?;
        self.store
            .delete(make_data_value_key(NAMESPACE, id))
            .await?;

        self.idx.delete(index_keys(&account.email).as_ref()).await?;

        Ok(account)
    }
}
