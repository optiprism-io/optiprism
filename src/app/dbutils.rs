use super::error::{Result, ERR_TODO};
use rocksdb::DB;
use std::{convert::TryInto, sync::Arc};

pub fn get_next_id(db: &Arc<DB>, key: &[u8]) -> Result<u64> {
    let mut id = 1u64;
    let value = db.get(key).unwrap();
    if let Some(value) = value {
        id += u64::from_le_bytes(value.try_into().unwrap());
    }
    db.put(key, id.to_le_bytes()).unwrap();
    Ok(id)
}
