use crate::store::index;
use crate::store::store::Store;
use crate::Result;
use bincode::{deserialize, serialize};
use chrono::Utc;
use std::sync::Arc;
use crate::accounts::{Account, ListAccountsRequest, UpdateAccountRequest};
use crate::accounts::types::CreateAccountRequest;
use crate::error::Error;

const NAMESPACE: &str = "accounts";
const IDX_EMAIL: &str = "email";

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    vec![
        Some(Key::Index(IDX_EMAIL, email).as_bytes()),
    ]
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
            Key::IdSequence => {
                [
                    NAMESPACE.as_bytes(),
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

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: index::hash_map::HashMap::new(kv),
        }
    }

    pub async fn create_account(&mut self, req: CreateAccountRequest) -> Result<Account> {
        let idx_keys = index_keys(req.email.as_str());
        self.idx
            .check_insert_constraints(idx_keys.as_ref())
            .await?;

        let created_at = Utc::now();
        let id = self.store.next_seq(Key::IdSequence.as_bytes()).await?;

        let account = req.into_account(id, created_at);

        self.store
            .put(
                Key::Data(account.id).as_bytes(),
                serialize(&account)?,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), account.id.to_le_bytes()).await?;
        Ok(account)
    }

    pub async fn get_account_by_id(&self, id: u64) -> Result<Account> {
        match self.store.get(Key::Data(id).as_bytes()).await? {
            None => Err(Error::AccountDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn get_account_by_email(&self, email: &str) -> Result<Account> {
        let id = self
            .idx
            .get(Key::Index(IDX_EMAIL, email).as_bytes())
            .await?;
        self.get_account_by_id(u64::from_le_bytes(id.try_into()?))
            .await
    }

    pub async fn list_accounts(&self, _request: ListAccountsRequest) -> Result<Vec<Account>> {
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

    pub async fn update_account(&mut self, req: UpdateAccountRequest) -> Result<Account> {
        let prev_account = self.get_account_by_id(req.id).await?;
        let idx_keys = index_keys(req.email.as_str());
        let idx_prev_keys = index_keys(prev_account.email.as_str());

        self.idx
            .check_update_constraints(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
            )
            .await?;

        let updated_at = Utc::now(); // TODO add updated_by
        let account = req.into_account(prev_account, updated_at, None);

        self.store
            .put(
                Key::Data(account.id).as_bytes(),
                serialize(&account)?,
            )
            .await?;

        self.idx
            .update(
                idx_keys.as_ref(),
                idx_prev_keys.as_ref(),
                account.id.to_le_bytes(),
            )
            .await?;

        Ok(account)
    }

    pub async fn delete_account(&mut self, id: u64) -> Result<Account> {
        let account = self.get_account_by_id(id).await?;
        self.store
            .delete(Key::Data(id).as_bytes())
            .await?;

        self.idx.delete(index_keys(account.email.as_str()).as_ref()).await?;
        Ok(account)
    }
}
