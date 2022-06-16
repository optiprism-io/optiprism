use std::sync::Arc;
use crate::Store;
use crate::error::{Result, Error};
use tokio::sync::RwLock;
use datafusion::parquet::data_type::AsBytes;
use crate::store::store::{make_id_seq_key, make_main_key};
use byteorder::{ByteOrder, LittleEndian};

const NAMESPACE: &[u8] = b"dictinaries";

fn dict_ns(dict: &str) -> Vec<u8> {
    [NAMESPACE, b"/", dict.as_bytes()].concat()
}

fn make_key_key(organization_id: u64, project_id: u64, dict: &str, key: u64) -> Vec<u8> {
    let main_key = make_main_key(organization_id, project_id, dict_ns(dict).as_slice());

    [main_key.as_slice(), b"keys/", key.to_le_bytes().as_ref()].concat()
}

fn make_value_key(organization_id: u64, project_id: u64, dict: &str, value: &str) -> Vec<u8> {
    let main_key = make_main_key(organization_id, project_id, dict_ns(dict).as_slice());

    [main_key.as_slice(), b"values/", value.as_bytes()].concat()
}

pub struct Provider {
    store: Arc<Store>,
    guard: RwLock<()>,
}

impl Provider {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            guard: RwLock::new(()),
        }
    }

    pub async fn get_key_or_create(&self, organization_id: u64, project_id: u64, dict: &str, value: &str) -> Result<u64> {
        // todo investigate hanging
        // self.guard.write().await;
        match self.store.get(make_value_key(organization_id, project_id, dict, value)).await? {
            None => {
                let id = self
                    .store
                    .next_seq(make_id_seq_key(organization_id, project_id, dict_ns(dict).as_slice()))
                    .await?;
                self.store.put(make_key_key(organization_id, project_id, dict, id), value.as_bytes()).await?;
                self.store.put(make_value_key(organization_id, project_id, dict, value), id.to_le_bytes().as_ref()).await?;

                Ok(id)
            }
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice()))
        }
    }

    pub async fn get_value(&self, organization_id: u64, project_id: u64, dict: &str, key: u64) -> Result<String> {
        let store_key = make_key_key(organization_id, project_id, dict, key);
        match self.store.get(store_key.as_slice()).await? {
            None => Err(Error::KeyNotFound(String::from_utf8(store_key)?)),
            Some(value) => {
                Ok(String::from_utf8(value)?)
            }
        }
    }

    pub async fn get_key(&self, organization_id: u64, project_id: u64, dict: &str, value: &str) -> Result<u64> {
        let store_key = make_value_key(organization_id, project_id, dict, value);
        match self.store.get(store_key.as_slice()).await? {
            None => Err(Error::KeyNotFound(String::from_utf8(store_key)?)),
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice()))
        }
    }
}

pub struct SingleDictionaryProvider {
    organization_id: u64,
    project_id: u64,
    dict: String,
    provider: Arc<Provider>,
}

impl SingleDictionaryProvider {
    pub fn new(organization_id: u64, project_id: u64, dict: String, provider: Arc<Provider>) -> Self {
        Self {
            organization_id,
            project_id,
            dict,
            provider,
        }
    }

    pub async fn get_key_or_create(&self, value: &str) -> Result<u64> {
        self.provider.get_key_or_create(self.organization_id, self.project_id, self.dict.as_str(), value).await
    }

    pub async fn get_value(&self, key: u64) -> Result<String> {
        self.provider.get_value(self.organization_id, self.project_id, self.dict.as_str(), key).await
    }

    pub async fn get_key(&self, organization_id: u64, project_id: u64, dict: &str, value: &str) -> Result<u64> {
        self.provider.get_key(self.organization_id, self.project_id, self.dict.as_str(), value).await
    }
}