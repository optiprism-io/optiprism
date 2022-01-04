use super::{Event, BASE_NAME, INDEX_NAME};
use crate::{
    common::sequence::{Sequence, SequenceBuilder},
    error::Result,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, Options, DB};
use std::sync::Arc;

pub struct Provider<'a> {
    db: &'a DB,
    base_cf: Arc<BoundColumnFamily<'a>>,
    index_cf: Arc<BoundColumnFamily<'a>>,
    sequence: Sequence<'a>,
}

impl<'a> Provider<'a> {
    pub fn cfs() -> Vec<ColumnFamilyDescriptor> {
        vec![
            ColumnFamilyDescriptor::new(BASE_NAME, Options::default()),
            ColumnFamilyDescriptor::new(INDEX_NAME, Options::default()),
        ]
    }

    pub fn new(db: &'a DB, sequence_builder: &'a SequenceBuilder) -> Self {
        Self {
            db,
            base_cf: db.cf_handle(BASE_NAME).unwrap(),
            index_cf: db.cf_handle(INDEX_NAME).unwrap(),
            sequence: sequence_builder.with_key(BASE_NAME.as_bytes()),
        }
    }

    pub fn create(&self, event: Event) -> Result<Event> {
        let id = self.sequence.next()?;
        let result = self.db.put_cf(
            &self.base_cf,
            id.to_le_bytes().as_ref(),
            serialize(&event).unwrap(),
        );
        if result.is_err() {
            unimplemented!();
        }
        Ok(event)
    }
}
