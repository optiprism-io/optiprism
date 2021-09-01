use super::error::{Result, ERR_TODO};
use parking_lot::Mutex;
use rocksdb::{ColumnFamily, DB};
use std::{convert::TryInto, sync::Arc};

pub struct Sequence {
    db: Arc<DB>,
    cf: Arc<ColumnFamily>,
    key: &'static [u8],
    guard: Mutex<()>,
}

impl Sequence {
    pub fn new(db: Arc<DB>, cf: Arc<ColumnFamily>, key: &'static [u8]) -> Self {
        let guard = Mutex::new(());
        Self { db, cf, key, guard }
    }

    pub fn next(&self) -> Result<u64> {
        let _guard = self.guard.lock();
        let mut id = 1u64;
        let value = match self.db.get_cf(self.cf.as_ref(), self.key) {
            Ok(value) => value,
            Err(_err) => return Err(ERR_TODO.into()),
        };
        if let Some(value) = value {
            id += u64::from_le_bytes(match value.try_into() {
                Ok(value) => value,
                Err(_err) => return Err(ERR_TODO.into()),
            });
        }
        let result = self.db.put_cf(self.cf.as_ref(), self.key, id.to_le_bytes());
        if result.is_err() {
            return Err(ERR_TODO.into());
        }
        Ok(id)
    }
}
