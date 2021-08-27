use super::error::Result;
use chrono::{DateTime, Utc};
use rocksdb::DB;
use std::collections::HashMap;
use std::sync::Arc;

enum Role {
    Admin,
    Owner,
    Reader,
}

enum Permission {
    List,
}

pub struct CreateRequest {
    pub organization_id: u64,
    pub username: String,
    pub password: String,
    pub roles: HashMap<String, Role>,
    pub permissions: HashMap<String, Vec<Permission>>,
}

pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub organization_id: u64,
    pub username: String,
    pub password: String,
    pub roles: HashMap<String, Role>,
    pub permissions: HashMap<String, Vec<Permission>>,
}

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

    pub fn create(&mut self, user: &Account) -> Result<Account> {
        unimplemented!()
    }

    pub fn get_by_id(&self, id: u64) -> Result<Account> {
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
