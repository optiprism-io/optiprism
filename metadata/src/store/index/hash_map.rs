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
        keys: &Vec<Option<Vec<u8>>>,
    ) -> Result<()> {
        for key in keys.iter() {
            if let Some(key) = key {
                if let Some(_) = self.store.get(key).await? {
                    return Err(Error::IndexKeyExist);
                }
            }
        }
        Ok(())
    }

    pub async fn insert<V: AsRef<[u8]>>(&mut self, keys: &Vec<Option<Vec<u8>>>, value: V) -> Result<()> {
        for key in keys.iter() {
            if let Some(key) = key {
                self.store.put(key, value.as_ref()).await?;
            }
        }
        Ok(())
    }

    pub async fn check_update_constraints(
        &mut self,
        keys: &Vec<Option<Vec<u8>>>,
        prev_keys: &Vec<Option<Vec<u8>>>,
    ) -> Result<()> {
        for (key, prev_key) in keys.iter().zip(prev_keys) {
            if let Some(key_v) = key {
                if key != prev_key {
                    if let Some(_) = self.store.get(key_v).await? {
                        return Err(Error::IndexKeyExist);
                    }
                }
            }
        }

        Ok(())
    }
    pub async fn update<V: AsRef<[u8]>>(
        &mut self,
        keys: &Vec<Option<Vec<u8>>>,
        prev_keys: &Vec<Option<Vec<u8>>>,
        value: V,
    ) -> Result<()> {
        for (key, prev_key) in keys.iter().zip(prev_keys) {
            if key != prev_key {
                if let Some(key) = prev_key {
                    self.store.delete(key).await?;
                }
            }

            if let Some(key) = key {
                self.store.put(key, value.as_ref()).await?;
            }
        }

        Ok(())
    }

    pub async fn delete(&mut self, keys: &Vec<Option<Vec<u8>>>) -> Result<()> {
        for key in keys.iter() {
            if let Some(key) = key {
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
