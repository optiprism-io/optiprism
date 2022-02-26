use super::{Account, CreateRequest, UpdateRequest};
use crate::metadata::{ListResponse, ResponseMetadata};
use crate::store::index::hash_map::HashMap;
use crate::store::store::Store;
use crate::{Error, Result};
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::RwLock;

const NAMESPACE: &[u8] = b"accounts";
const IDX_EMAIL: &[u8] = b"email";

pub fn make_id_seq_key(ns: &[u8]) -> Vec<u8> {
    [ns, b"/id_seq"].concat()
}

fn make_index_key(ns: &[u8], idx_name: &[u8], key: &str) -> Vec<u8> {
    [ns, b"/idx/", idx_name, b"/", key.as_bytes()].concat()
}

pub fn make_data_key(ns: &[u8]) -> Vec<u8> {
    [ns, b"/data/"].concat()
}

pub fn make_data_value_key(ns: &[u8], id: u64) -> Vec<u8> {
    [make_data_key(ns).as_slice(), id.to_le_bytes().as_ref()].concat()
}

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    let mut idx: Vec<Option<Vec<u8>>> = vec![];
    idx.push(Some(make_index_key(NAMESPACE, IDX_EMAIL, email).to_vec()));

    idx
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

    pub async fn create(&self, req: CreateRequest) -> Result<Account> {
        let _guard = self.guard.write().await;
        let idx_keys = index_keys(req.email.as_str());
        self.idx.check_insert_constraints(idx_keys.as_ref()).await?;
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
        match self.store.get(make_data_value_key(NAMESPACE, id)).await? {
            None => Err(Error::KeyNotFound),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_email(&self, email: &str) -> Result<Account> {
        let _guard = self.guard.read().await;
        let data = self
            .idx
            .get(make_index_key(NAMESPACE, IDX_EMAIL, email))
            .await?;

        Ok(deserialize(&data)?)
    }

    pub async fn list(&self) -> Result<ListResponse<Account>> {
        let prefix = make_data_key(NAMESPACE);

        let list = self
            .store
            .list_prefix("")
            .await?
            .iter()
            .filter_map(|x| {
                if x.0.len() < prefix.len() || !prefix.as_slice().cmp(&x.0[..prefix.len()]).is_eq()
                {
                    return None;
                }

                Some(deserialize(x.1.as_ref()))
            })
            .collect::<bincode::Result<_>>()?;

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub async fn update(&self, req: UpdateRequest) -> Result<Account> {
        let _guard = self.guard.write().await;
        let prev_account = self.get_by_id(req.id).await?;
        let idx_keys = index_keys(req.email.as_str());
        let idx_prev_keys = index_keys(prev_account.email.as_str());
        self.idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;
        let updated_at = Utc::now(); // TODO add updated_by
        let account = req.into_account(prev_account, updated_at, None);
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
        self.idx
            .delete(index_keys(account.email.as_str()).as_ref())
            .await?;
        Ok(account)
    }
}
