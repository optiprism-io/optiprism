use super::{
    entity_utils::List,
    error::{Result, ERR_ORGANIZATION_NOT_FOUND, ERR_TODO},
    sequence::Sequence,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

pub const COLUMN_FAMILY: &str = "organization";
const SEQUENCE_KEY: &'static [u8] = b"organization";

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
    // TODO: add create organization fields
}

pub struct Provider {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    sequence: Sequence,
}

impl Provider {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let sequence = Sequence::new(db.clone(), SEQUENCE_KEY)?;
        let dcf = match db.cf_handle(COLUMN_FAMILY) {
            Some(dfc) => dfc,
            None => return Err(ERR_TODO.into()),
        };
        let cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(dcf) };
        Ok(Self { db, cf, sequence })
    }

    pub fn create(&self, request: CreateRequest) -> Result<Organization> {
        let id = self.sequence.next()?;
        let org = Organization {
            id,
            created_at: Utc::now(),
            updated_at: None,
            name: request.name,
        };
        self.db
            .put_cf(
                self.cf.as_ref(),
                id.to_le_bytes().as_ref(),
                serialize(&org).unwrap(),
            )
            .unwrap();
        Ok(org)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Organization> {
        let value = self
            .db
            .get_cf(self.cf.as_ref(), id.to_le_bytes().as_ref())
            .unwrap();
        if let Some(value) = value {
            return Ok(deserialize(&value).unwrap());
        }
        return Err(ERR_ORGANIZATION_NOT_FOUND.into());
    }

    pub fn list(&self) -> Result<List<Organization>> {
        unimplemented!()
    }

    pub fn update(&mut self, user: &Organization) -> Result<Organization> {
        unimplemented!()
    }

    pub fn delete(&mut self, id: u64) -> Result<()> {
        unimplemented!()
    }
}
