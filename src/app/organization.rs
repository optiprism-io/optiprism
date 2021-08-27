use super::error::Result;
use chrono::{DateTime, Utc};
use rocksdb::DB;
use std::sync::Arc;

pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub name: String,
    // TODO: add organization fields
}

pub struct CreateRequest {
    pub name: String,
    // TODO: add organization fields
}

pub struct List {
    pub data: Vec<Organization>,
    pub total: u64,
}

pub struct Provider {
    db: Arc<DB>,
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Self {
        Provider { db }
    }

    pub fn create(&self, request: CreateRequest) -> Result<Organization> {
        unimplemented!()
    }

    pub fn get_by_id(&self, id: u64) -> Result<Organization> {
        unimplemented!()
    }

    pub fn list(&self) -> Result<List> {
        unimplemented!()
    }

    pub fn update(&mut self, user: &Organization) -> Result<Organization> {
        unimplemented!()
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        unimplemented!()
    }
}
