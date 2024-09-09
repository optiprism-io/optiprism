
use rocksdb::Transaction;
use rocksdb::TransactionDB;

use crate::error::MetadataError;
use crate::Result;

pub fn check_insert_constraints(
    tx: &Transaction<TransactionDB>,
    keys: &[Option<Vec<u8>>],
) -> Result<()> {
    for key in keys.iter().flatten() {
        if tx.get(key)?.is_some() {
            return Err(MetadataError::AlreadyExists(String::from_utf8(
                key.to_owned(),
            )?));
        }
    }
    Ok(())
}

pub fn insert_index(
    tx: &Transaction<TransactionDB>,
    keys: &[Option<Vec<u8>>],
    id: u64,
) -> Result<()> {
    for key in keys.iter().flatten() {
        tx.put(key, id.to_le_bytes())?;
    }
    Ok(())
}

pub fn check_update_constraints(
    tx: &Transaction<TransactionDB>,
    keys: &[Option<Vec<u8>>],
    prev_keys: &[Option<Vec<u8>>],
) -> Result<()> {
    for (key, prev_key) in keys.iter().zip(prev_keys) {
        if let Some(key_v) = key {
            if key != prev_key && (tx.get(key_v)?).is_some() {
                return Err(MetadataError::AlreadyExists(String::from_utf8(
                    key_v.to_owned(),
                )?));
            }
        }
    }

    Ok(())
}

pub fn update_index(
    tx: &Transaction<TransactionDB>,
    keys: &[Option<Vec<u8>>],
    prev_keys: &[Option<Vec<u8>>],
    value: u64,
) -> Result<()> {
    for (key, prev_key) in keys.iter().zip(prev_keys) {
        if key != prev_key {
            if let Some(key) = prev_key {
                tx.delete(key)?;
            }
        }

        if let Some(key) = key {
            tx.put(key, value.to_le_bytes())?;
        }
    }

    Ok(())
}

pub fn delete_index(tx: &Transaction<TransactionDB>, keys: &[Option<Vec<u8>>]) -> Result<()> {
    for key in keys.iter().flatten() {
        tx.delete(key)?;
    }
    Ok(())
}

pub fn get_index<K>(
    tx: &Transaction<TransactionDB>,
    key: K,
    err_key: impl ToString,
) -> Result<u64>
where
    K: AsRef<[u8]>,
{
    match tx.get(key.as_ref())? {
        None => Err(MetadataError::NotFound(err_key.to_string())),
        Some(v) => Ok(u64::from_le_bytes(v.try_into().unwrap())),
    }
}

pub fn next_seq<K: AsRef<[u8]>>(tx: &Transaction<TransactionDB>, key: K) -> Result<u64> {
    let id = tx.get(key.as_ref())?;
    let result: u64 = match id {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()) + 1,
        None => 1,
    };
    tx.put(key, result.to_le_bytes())?;

    Ok(result)
}

pub fn next_zero_seq<K: AsRef<[u8]>>(tx: &Transaction<TransactionDB>, key: K) -> Result<u64> {
    let id = tx.get(key.as_ref())?;
    let result: u64 = match id {
        Some(v) => u64::from_le_bytes(v.try_into().unwrap()) + 1,
        None => 0,
    };
    tx.put(key, result.to_le_bytes())?;

    Ok(result)
}
