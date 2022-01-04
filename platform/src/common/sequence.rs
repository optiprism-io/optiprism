use crate::error::Result;
use parking_lot::Mutex;
use rocksdb::{BoundColumnFamily, DB};
use std::{convert::TryInto, sync::Arc};

pub struct SequenceBuilder<'a> {
    db: &'a DB,
    cf: Arc<BoundColumnFamily<'a>>,
}

impl<'a> SequenceBuilder<'a> {
    pub fn new(db: &'a DB, cf: Arc<BoundColumnFamily<'a>>) -> Self {
        Self { db, cf }
    }

    pub fn with_key(&self, key: &'a [u8]) -> Sequence<'a> {
        Sequence::new(self.db, self.cf.clone(), key)
    }
}

pub struct Sequence<'a> {
    db: &'a DB,
    cf: Arc<BoundColumnFamily<'a>>,
    key: &'a [u8],
    guard: Mutex<()>,
}

impl<'a> Sequence<'a> {
    pub fn new(db: &'a DB, cf: Arc<BoundColumnFamily<'a>>, key: &'a [u8]) -> Self {
        let guard = Mutex::new(());
        Self { db, cf, key, guard }
    }

    pub fn next(&self) -> Result<u64> {
        let _guard = self.guard.lock();
        let mut id = 1u64;
        let value = match self.db.get_cf(&self.cf, self.key) {
            Ok(value) => value,
            Err(_err) => unimplemented!(),
        };
        if let Some(value) = value {
            id += u64::from_le_bytes(match value.try_into() {
                Ok(value) => value,
                Err(_err) => unimplemented!(),
            });
        }
        let result = self.db.put_cf(&self.cf, self.key, id.to_le_bytes());
        if result.is_err() {
            unimplemented!();
        }
        Ok(id)
    }
}
