use crate::error::Error;
use crate::store::store::Store;
use crate::Result;
use std::sync::Arc;

pub struct HashMap {
    store: Arc<Store>,
}

impl HashMap {
    pub fn new(store: Arc<Store>) -> Self {
        HashMap { store }
    }

    pub async fn check_insert_constraints(
        &mut self,
        idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<()> {
        for kv in idx.iter() {
            if let Some((key, _)) = kv {
                if let Some(_) = self.store.get(key).await? {
                    return Err(Error::IndexKeyExist);
                }
            }
        }
        Ok(())
    }

    pub async fn insert(&mut self, idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>) -> Result<()> {
        for kv in idx.iter() {
            if let Some((key, value)) = kv {
                self.store.put(key, value).await?;
            }
        }
        Ok(())
    }

    pub async fn check_update_constraints(
        &mut self,
        idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>,
        prev_idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<()> {
        for (kv, prev_kv) in idx.iter().zip(prev_idx) {
            if let Some((key, _)) = kv {
                if kv != prev_kv {
                    if let Some(_) = self.store.get(key).await? {
                        return Err(Error::IndexKeyExist);
                    }
                }
            }
        }

        Ok(())
    }
    pub async fn update(
        &mut self,
        idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>,
        prev_idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>,
    ) -> Result<()> {
        for (kv, prev_kv) in idx.iter().zip(prev_idx) {
            if kv != prev_kv {
                if let Some((key, _)) = prev_kv {
                    self.store.delete(key).await?;
                }
            }

            if let Some((key, value)) = kv {
                self.store.put(key, value).await?;
            }
        }

        Ok(())
    }

    pub async fn delete(&mut self, idx: &Vec<Option<(Vec<u8>, Vec<u8>)>>) -> Result<()> {
        for kv in idx.iter() {
            if let Some((key, _)) = kv {
                self.store.delete(key).await?;
            }
        }
        Ok(())
    }

    pub async fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        match self.store.get(key).await? {
            None => Err(Error::IndexKeyNotFound),
            Some(v) => Ok(v),
        }
    }
}
