use crate::error::StoreError;
use crate::store::Store;
use crate::Result;
use std::sync::Arc;

#[derive(Clone)]
pub struct HashMap {
    store: Arc<Store>,
}

impl HashMap {
    pub fn new(store: Arc<Store>) -> Self {
        HashMap { store }
    }

    pub async fn check_insert_constraints(&self, keys: &[Option<Vec<u8>>]) -> Result<()> {
        for key in keys.iter().flatten() {
            if (self.store.get(key).await?).is_some() {
                return Err(
                    StoreError::KeyAlreadyExists(String::from_utf8(key.to_owned())?).into(),
                );
            }
        }
        Ok(())
    }

    pub async fn insert<V: AsRef<[u8]>>(&self, keys: &[Option<Vec<u8>>], value: V) -> Result<()> {
        for key in keys.iter().flatten() {
            self.store.put(key, value.as_ref()).await?;
        }
        Ok(())
    }

    pub async fn check_update_constraints(
        &self,
        keys: &[Option<Vec<u8>>],
        prev_keys: &[Option<Vec<u8>>],
    ) -> Result<()> {
        for (key, prev_key) in keys.iter().zip(prev_keys) {
            if let Some(key_v) = key {
                if key != prev_key && (self.store.get(key_v).await?).is_some() {
                    return Err(
                        StoreError::KeyAlreadyExists(String::from_utf8(key_v.to_owned())?).into(),
                    );
                }
            }
        }

        Ok(())
    }
    pub async fn update<V: AsRef<[u8]>>(
        &self,
        keys: &[Option<Vec<u8>>],
        prev_keys: &[Option<Vec<u8>>],
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

    pub async fn delete(&self, keys: &[Option<Vec<u8>>]) -> Result<()> {
        for key in keys.iter().flatten() {
            self.store.delete(key).await?;
        }
        Ok(())
    }

    pub async fn get<K>(&self, key: K) -> Result<Vec<u8>>
    where
        K: AsRef<[u8]>,
    {
        match self.store.get(key.as_ref()).await? {
            None => {
                Err(StoreError::KeyNotFound(String::from_utf8(key.as_ref().to_owned())?).into())
            }
            Some(v) => Ok(v),
        }
    }
}
