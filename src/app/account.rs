use super::{
    auth::{make_password_hash, make_salt},
    error::{Result, ERR_TODO},
    rbac::{Permission, Role, Scope},
};
use chrono::{DateTime, Utc};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{Arc, Mutex},
};

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub admin: bool,
    pub salt: String,
    pub password: String,
    pub organization_id: u64,
    pub username: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
}

#[derive(Deserialize)]
pub struct CreateRequest {
    pub admin: bool,
    pub password: String,
    pub organization_id: u64,
    pub username: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
}

#[derive(Deserialize)]
struct List {
    pub data: Vec<Account>,
    pub total: u64,
}

pub struct Provider {
    db: Arc<DB>,
    sequence_guard: Mutex<()>,
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Self {
        Provider {
            db,
            sequence_guard: Mutex::new(()),
        }
    }

    pub fn create(&self, request: CreateRequest) -> Result<Account> {
        let id = {
            let _guard = self.sequence_guard.lock().unwrap();
            self.db
                .merge("account_sequence_number", 1u64.to_le_bytes())
                .unwrap();
            u64::from_le_bytes(
                self.db
                    .get("account_sequence_number")
                    .unwrap()
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        };
        let salt = make_salt();
        let password = make_password_hash(&request.password, &salt);
        let acc = Account {
            id,
            created_at: Utc::now(),
            updated_at: None,
            admin: request.admin,
            salt,
            password,
            organization_id: request.organization_id,
            username: request.username,
            roles: request.roles,
            permissions: request.permissions,
        };
        self.db
            .put(
                [b"account:".as_ref(), id.to_le_bytes().as_ref()].concat(),
                serde_json::to_vec(&acc).unwrap(),
            )
            .unwrap();
        Ok(acc)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Account> {
        let value = self
            .db
            .get([b"account:".as_ref(), id.to_le_bytes().as_ref()].concat())
            .unwrap();
        if let Some(value) = value {
            return Ok(serde_json::from_slice(&value).unwrap());
        }
        return Err(ERR_TODO.into());
    }

    pub fn get_by_email(&self, email: String) -> Result<Account> {
        unimplemented!()
    }

    pub fn list(&self) -> Result<List> {
        unimplemented!()
    }

    pub fn update(&mut self, user: &Account) -> Result<Account> {
        unimplemented!()
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        unimplemented!()
    }
}
