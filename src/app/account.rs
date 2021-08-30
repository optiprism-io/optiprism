use super::{
    auth::{make_password_hash, make_salt},
    entity_utils::List,
    error::{Result, ERR_ACCOUNT_NOT_FOUND, ERR_TODO},
    rbac::{Permission, Role, Scope},
    sequence::Sequence,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub const COLUMN_FAMILY: &str = "account";
const SEQUENCE_KEY: &'static [u8] = b"account";

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
    // TODO: add account fields
}

#[derive(Deserialize)]
pub struct CreateRequest {
    pub admin: bool,
    pub password: String,
    pub organization_id: u64,
    pub username: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
    // TODO: add create account fields
}

pub struct Provider {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    sequence: Sequence,
    create_guard: Mutex<()>,
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let sequence = Sequence::new(db.clone(), SEQUENCE_KEY)?;
        let dcf = match db.cf_handle(COLUMN_FAMILY) {
            Some(dfc) => dfc,
            None => return Err(ERR_TODO.into()),
        };
        let cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(dcf) };
        Ok(Self {
            db,
            cf,
            sequence,
            create_guard: Mutex::new(()),
        })
    }

    pub fn create(&self, request: CreateRequest) -> Result<Account> {
        let id = self.sequence.next()?;
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
            .put_cf(
                self.cf.as_ref(),
                id.to_le_bytes().as_ref(),
                serialize(&acc).unwrap(),
            )
            .unwrap();
        Ok(acc)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Account> {
        let value = self
            .db
            .get_cf(self.cf.as_ref(), id.to_le_bytes().as_ref())
            .unwrap();
        if let Some(value) = value {
            return Ok(deserialize(&value).unwrap());
        }
        return Err(ERR_ACCOUNT_NOT_FOUND.into());
    }

    pub fn get_by_email(&self, email: String) -> Result<Account> {
        unimplemented!()
    }

    pub fn list(&self) -> Result<List<Account>> {
        unimplemented!()
    }

    pub fn update(&mut self, user: &Account) -> Result<Account> {
        unimplemented!()
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        unimplemented!()
    }
}
