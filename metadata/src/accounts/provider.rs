use crate::store::{index, store};
use crate::store::store::Store;
use crate::Result;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use types::account::{Account, CreateAccountRequest, ListRequest, UpdateRequest};

const NAMESPACE: &str = "accounts";
const IDX_EMAIL: &str = "email";

#[derive(Clone)]
pub enum Key<'a> {
    // {namespace}/data/{account_id}
    Data(&'a str, u64),
    // {namespace}/idx//{idx_name}/{idx_value}
    Index(&'a str, &'a str, &'a str),
    // {namespace}/id_seq
    IdSequence(&'a str),
}

impl Key<'a> {
    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            Key::Data(ns, account_id) => [
                ns.as_bytes(),
                b"/data/",
                account_id.to_le_bytes().as_ref(),
            ]
                .concat(),
            Key::Index(ns, idx_name, key) => [
                ns.as_bytes(),
                b"/idx/",
                idx_name.as_bytes(),
                b"/",
                key,
            ]
                .concat(),
            Key::IdSequence(ns) => {
                [
                    ns.as_bytes(),
                    b"/id_seq",
                ].concat()
            }
        }
    }
}

pub struct Provider {
    store: Arc<Store>,
    idx: index::hash_map::HashMap,
}

fn indexes(account: &Account) -> Vec<Option<(Vec<u8>, Vec<u8>)>> {
    vec![Some((
        Key::Index(NAMESPACE, IDX_EMAIL, account.email.as_str()).as_bytes(),
        account.id.to_le_bytes().to_vec(),
    ))]
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: idx: index::hash_map::HashMap::new(kv),
        }
    }

    pub async fn create_account(&self, request: CreateAccountRequest) -> Result<Account> {
        self.idx
            .check_insert_constraints(indexes(&event).as_ref())
            .await?;
        let account = Account {
            id: self.store.next_seq(KV_NAMESPACE).await?,
            created_at: Utc::now(),
            updated_at: None,
            admin: request.admin,
            salt: request.salt,
            password: request.password,
            organization_id: request.organization_id,
            email: request.email,
            roles: request.roles,
            permissions: request.permissions,
            first_name: request.first_name,
            middle_name: request.middle_name,
            last_name: request.last_name,
        };
        self.store
            .put(KV_NAMESPACE, account.id.to_le_bytes(), serialize(&account)?)
            .await?;
        Ok(account)
    }

    pub async fn get_by_id(&self, id: u64) -> Result<Option<Account>> {
        Ok(
            match self.store.get(KV_NAMESPACE, id.to_le_bytes()).await? {
                None => None,
                Some(value) => Some(deserialize(&value)?),
            },
        )
    }

    pub async fn get_by_email(&self, _email: &str) -> Result<Option<Account>> {
        unimplemented!()
    }

    pub async fn list(&self, _request: ListRequest) -> Result<Vec<Account>> {
        // TODO: apply limit/offset
        let list = self
            .store
            .list(KV_NAMESPACE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;
        Ok(list)
    }

    pub async fn update(&self, request: UpdateRequest) -> Result<Option<Account>> {
        // TODO: lock
        let mut account = match self.get_by_id(request.id).await? {
            Some(account) => account,
            None => unimplemented!(),
        };
        let mut updated = false;
        if let Some(value) = &request.first_name {
            updated = true;
            account.first_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if let Some(value) = &request.middle_name {
            updated = true;
            account.middle_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if let Some(value) = &request.last_name {
            updated = true;
            account.last_name = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        if updated {
            account.updated_at = Some(Utc::now());
        }

        Ok(
            match self
                .store
                .put_checked(KV_NAMESPACE, account.id.to_le_bytes(), serialize(&account)?)
                .await?
            {
                None => None,
                Some(_) => Some(account),
            },
        )
    }

    pub async fn delete(&self, id: u64) -> Result<Option<Account>> {
        Ok(
            match self
                .store
                .delete_checked(KV_NAMESPACE, id.to_le_bytes())
                .await?
            {
                None => None,
                Some(v) => Some(deserialize(&v)?),
            },
        )
    }
}
