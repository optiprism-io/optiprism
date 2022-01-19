use std::sync::Arc;
use crate::store::store::Store;
use crate::Result;
use crate::error::Error;

pub struct HashMap {
    store: Arc<Store>,
}

fn make_index_key(idx_name: &[u8], key: &[u8]) -> Vec<u8> {
    [idx_name, b":", key].concat()
}

pub type IndexKV = (Vec<u8>, Option<(Vec<u8>, Vec<u8>)>);

impl HashMap {
    pub fn new(store: Arc<Store>) -> Self {
        HashMap { store }
    }

    pub async fn check_insert_constraints(&mut self, idx_kv: Vec<IndexKV>) -> Result<()> {
        for (idx_name, kv) in idx_kv.iter() {
            if let Some((key, _)) = kv {
                if let Some(_) = self.store.get(make_index_key(idx_name, key)).await? {
                    return Err(Error::IndexKeyExist);
                }
            }
        }
        Ok(())
    }

    pub async fn insert(&mut self, idx_kv: Vec<IndexKV>) -> Result<()> {
        for (idx_name, kv) in idx_kv.iter() {
            if let Some((key, value)) = kv {
                self.store.put(make_index_key(idx_name, key), value).await?;
            }
        }
        Ok(())
    }

    pub async fn check_update_constraints(&mut self, idx_kv: Vec<IndexKV>, prev_idx_kv: Vec<IndexKV>) -> Result<()> {
        for ((idx_name, kv), (_, prev_kv)) in idx_kv.iter().zip(&prev_idx_kv) {
            if let Some((key, _)) = kv {
                if kv != prev_kv {
                    if let Some(_) = self.store.get(make_index_key(idx_name, key)).await? {
                        return Err(Error::IndexKeyExist);
                    }
                }
            }
        }

        Ok(())
    }
    pub async fn update(&mut self, idx_kv: Vec<IndexKV>, prev_idx_kv: Vec<IndexKV>) -> Result<()> {
        for ((idx_name, kv), (_, prev_kv)) in idx_kv.iter().zip(&prev_idx_kv) {
            if kv != prev_kv {
                if let Some((key, _)) = prev_kv {
                    self.store.delete(make_index_key(idx_name, key)).await?;
                }
            }

            if let Some((key, value)) = kv {
                self.store.put(make_index_key(idx_name, key), value).await?;
            }
        }

        Ok(())
    }

    pub async fn delete(&mut self, idx_kv: Vec<IndexKV>) -> Result<()> {
        for (idx_name, kv) in idx_kv.iter() {
            if let Some((key, _)) = kv {
                self.store.delete(make_index_key(idx_name, key)).await?;
            }
        }
        Ok(())
    }

    pub async fn get<K>(&self, idx_name: &[u8], key: K) -> Result<Vec<u8>> where
        K: AsRef<[u8]> {
        match self.store.get(make_index_key(idx_name, key.as_ref())).await? {
            None => Err(Error::IndexKeyNotFound),
            Some(v) => Ok(v)
        }
    }
}