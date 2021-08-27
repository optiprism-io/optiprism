use super::{
    error::Result,
    rbac::{Permission, Role, Scope},
};
use chrono::{DateTime, Utc};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub salt: String,
    pub password: String,
    pub admin: bool,
    pub organization_id: u64,
    pub username: String,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
}

#[derive(Deserialize)]
pub struct CreateRequest {
    pub organization_id: u64,
    pub username: String,
    pub password: String,
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
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Self {
        Provider { db }
    }

    pub fn create(&self, request: CreateRequest) -> Result<Account> {
        unimplemented!()
    }

    pub fn get_by_id(&self, id: u64) -> Result<Account> {
        unimplemented!()
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
