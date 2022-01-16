use super::{
    entity_utils::List,
    error::{Result, ERR_ORGANIZATION_NOT_FOUND, ERR_TODO},
    sequence::Sequence,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, DB};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const PRIMARY_CF: &str = "organization";
pub const SECONDARY_CF: &str = "organization_sec";
const SEQUENCE_KEY: &'static [u8] = b"_id";

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
    primary_cf: Arc<ColumnFamily>,
    secondary_cf: Arc<ColumnFamily>,
    sequence: Sequence,
    create_guard: Mutex<()>,
}

unsafe impl Send for Provider {}
unsafe impl Sync for Provider {}

impl Provider {
    pub fn new(db: Arc<DB>) -> Result<Self> {
        let bcf = match db.cf_handle(PRIMARY_CF) {
            Some(bcf) => bcf,
            None => return Err(ERR_TODO.into()),
        };
        let primary_cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(bcf) };
        let bcf = match db.cf_handle(SECONDARY_CF) {
            Some(bcf) => bcf,
            None => return Err(ERR_TODO.into()),
        };
        let secondary_cf: Arc<ColumnFamily> = unsafe { std::mem::transmute(bcf) };
        let sequence = Sequence::new(db.clone(), secondary_cf.clone(), SEQUENCE_KEY);
        Ok(Self {
            db,
            primary_cf,
            secondary_cf,
            sequence,
            create_guard: Mutex::new(()),
        })
    }

    pub fn create(&self, request: CreateRequest) -> Result<Organization> {
        let id = self.sequence.next()?;
        let org = Organization {
            id,
            created_at: Utc::now(),
            updated_at: None,
            name: request.name,
        };
        let result = self.db.put_cf(
            self.primary_cf.as_ref(),
            id.to_le_bytes().as_ref(),
            serialize(&org).unwrap(),
        );
        if result.is_err() {
            return Err(ERR_TODO.into());
        }
        Ok(org)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Organization> {
        let value = self
            .db
            .get_cf(self.primary_cf.as_ref(), id.to_le_bytes().as_ref())
            .unwrap();
        if let Some(value) = value {
            return Ok(deserialize(&value).unwrap());
        }
        Err(ERR_ORGANIZATION_NOT_FOUND.into())
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
