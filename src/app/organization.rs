use super::{
    dbutils::get_next_id,
    error::{Result, ERR_TODO},
};
use chrono::{DateTime, Utc};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize)]
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
    sequence_guard: Mutex<()>,
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Self {
        Provider {
            db,
            sequence_guard: Mutex::new(()),
        }
    }

    pub fn create(&self, request: CreateRequest) -> Result<Organization> {
        let id = {
            let _guard = self.sequence_guard.lock().unwrap();
            get_next_id(&self.db, b"organization_sequence_number")?
        };
        let org = Organization {
            id,
            created_at: Utc::now(),
            updated_at: None,
            name: request.name,
        };
        self.db
            .put(
                [b"organization:".as_ref(), id.to_le_bytes().as_ref()].concat(),
                serde_json::to_vec(&org).unwrap(),
            )
            .unwrap();
        Ok(org)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Organization> {
        let value = self
            .db
            .get([b"organization:".as_ref(), id.to_le_bytes().as_ref()].concat())
            .unwrap();
        if let Some(value) = value {
            return Ok(serde_json::from_slice(&value).unwrap());
        }
        return Err(ERR_TODO.into());
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
