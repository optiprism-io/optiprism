use super::types::{Account, CreateRequest, ListRequest, UpdateRequest};
use crate::{
    kv::{self, KV},
    Result,
};
use async_trait::async_trait;
use bincode::{deserialize, serialize};
use chrono::Utc;

const KV_TABLE: kv::Table = kv::Table::Accounts;

pub struct Provider {
    kv: KV,
}

#[async_trait]
impl super::Provider for Provider {
    async fn create(&self, request: CreateRequest) -> Result<Account> {
        let account = Account {
            id: self.kv.next_seq(KV_TABLE).await?,
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
        self.kv
            .put(
                KV_TABLE,
                account.id.to_le_bytes().as_ref(),
                serialize(&account)?.as_ref(),
            )
            .await?;
        Ok(account)
    }

    async fn get_by_id(&self, id: u64) -> Result<Option<Account>> {
        Ok(
            match self.kv.get(KV_TABLE, id.to_le_bytes().as_ref()).await? {
                None => None,
                Some(value) => Some(deserialize(&value)?),
            },
        )
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<Account>> {
        unimplemented!()
    }

    async fn list(&self, request: ListRequest) -> Result<Vec<Account>> {
        // TODO: apply limit/offset
        let list = self
            .kv
            .list(KV_TABLE)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<_>>()?;
        Ok(list)
    }

    async fn update(&self, request: UpdateRequest) -> Result<Account> {
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
            self.kv
                .put(
                    KV_TABLE,
                    account.id.to_le_bytes().as_ref(),
                    serialize(&account)?.as_ref(),
                )
                .await?;
        }
        if updated {
            account.updated_at = Some(Utc::now());
            self.kv
                .put(
                    KV_TABLE,
                    account.id.to_le_bytes().as_ref(),
                    serialize(&account)?.as_ref(),
                )
                .await?;
        }
        Ok(account)
    }

    async fn delete(&self, id: u64) -> Result<()> {
        Ok(self.kv.delete(KV_TABLE, id.to_le_bytes().as_ref()).await?)
    }
}
