use super::{Account, CreateRequest, ListRequest, UpdateRequest};
use crate::store::index::hash_map::HashMap;
use crate::store::store::Store;
use crate::{Error, Result};
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::RwLock;

const NAMESPACE: &str = "accounts";
const IDX_EMAIL: &str = "email";

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    vec![Some(Key::Index(IDX_EMAIL, email).as_bytes())]
}

#[derive(Clone)]
pub enum Key<'a> {
    // accounts/data/{account_id}
    Data(u64),
    // accounts/idx/{idx_name}/{idx_value}
    Index(&'a str, &'a str),
    // accounts/id_seq
    IdSequence,
}

impl<'a> Key<'a> {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Key::Data(account_id) => [
                NAMESPACE.as_bytes(),
                b"/data/",
                account_id.to_le_bytes().as_ref(),
            ]
            .concat(),
            Key::Index(idx_name, key) => [
                NAMESPACE.as_bytes(),
                b"/idx/",
                idx_name.as_bytes(),
                b"/",
                key.as_bytes(),
            ]
            .concat(),
            Key::IdSequence => [NAMESPACE.as_bytes(), b"/id_seq"].concat(),
        }
    }
}

pub struct Provider {
    store: Arc<Store>,
    idx: *mut HashMap,
    guard: RwLock<()>,
}

unsafe impl Send for Provider {}
unsafe impl Sync for Provider {}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: Box::leak(Box::new(HashMap::new(kv))),
            guard: RwLock::new(()),
        }
    }

    pub async fn create(&self, req: CreateRequest) -> Result<Account> {
        let _guard = self.guard.write().await;
        let idx = unsafe { self.idx.as_mut().unwrap() };
        let idx_keys = index_keys(req.email.as_str());
        idx.check_insert_constraints(idx_keys.as_ref()).await?;

        let created_at = Utc::now();
        let id = self.store.next_seq(Key::IdSequence.as_bytes()).await?;
        let account = req.into_account(id, created_at);
        self.store
            .put(Key::Data(account.id).as_bytes(), serialize(&account)?)
            .await?;
        idx.insert(idx_keys.as_ref(), account.id.to_le_bytes())
            .await?;
        Ok(account)
    }

    pub async fn get_by_id(&self, id: u64) -> Result<Account> {
        match self.store.get(Key::Data(id).as_bytes()).await? {
            None => Err(Error::AccountDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_by_email(&self, email: &str) -> Result<Account> {
        let _guard = self.guard.read().await;
        let idx = unsafe { self.idx.as_ref().unwrap() };
        let id = idx.get(Key::Index(IDX_EMAIL, email).as_bytes()).await?;
        self.get_by_id(u64::from_le_bytes(id.try_into()?)).await
    }

    pub async fn list_accounts(&self, _request: ListRequest) -> Result<Vec<Account>> {
        /*// TODO: apply limit/offset
        let list = self
            .store
            .list(KV_NAMESPACE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;
        Ok(list)*/
        unimplemented!()
    }

    pub async fn update(&self, req: UpdateRequest) -> Result<Account> {
        let _guard = self.guard.write().await;
        let idx = unsafe { self.idx.as_mut().unwrap() };
        let prev_account = self.get_by_id(req.id).await?;
        let idx_keys = index_keys(req.email.as_str());
        let idx_prev_keys = index_keys(prev_account.email.as_str());
        idx.check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await?;
        let updated_at = Utc::now(); // TODO add updated_by
        let account = req.into_account(prev_account, updated_at, None);
        self.store
            .put(Key::Data(account.id).as_bytes(), serialize(&account)?)
            .await?;
        idx.update(
            idx_keys.as_ref(),
            idx_prev_keys.as_ref(),
            account.id.to_le_bytes(),
        )
        .await?;
        Ok(account)
    }

    pub async fn delete(&self, id: u64) -> Result<Account> {
        let _guard = self.guard.write().await;
        let idx = unsafe { self.idx.as_mut().unwrap() };
        let account = self.get_by_id(id).await?;
        self.store.delete(Key::Data(id).as_bytes()).await?;
        idx.delete(index_keys(account.email.as_str()).as_ref())
            .await?;
        Ok(account)
    }
}
