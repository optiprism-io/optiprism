use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use async_trait::async_trait;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use tokio::sync::RwLock;

use crate::dictionaries::Provider;
use crate::error::DictionaryError;
use crate::error::DictionaryKey;
use crate::error::DictionaryValue;
use crate::error::Result;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;

const NAMESPACE: &[u8] = b"dictinaries";

fn dict_ns(dict: &str) -> Vec<u8> {
    [NAMESPACE, b"/", dict.as_bytes()].concat()
}

fn make_key_key(organization_id: u64, project_id: u64, dict: &str, key: u64) -> Vec<u8> {
    [
        org_proj_ns(organization_id, project_id, dict_ns(dict).as_slice()).as_slice(),
        b"keys/",
        key.to_le_bytes().as_ref(),
    ]
    .concat()
}

fn make_value_key(organization_id: u64, project_id: u64, dict: &str, value: &str) -> Vec<u8> {
    [
        org_proj_ns(organization_id, project_id, dict_ns(dict).as_slice()).as_slice(),
        b"values/",
        value.as_bytes(),
    ]
    .concat()
}

#[derive(Debug)]
pub struct ProviderImpl {
    store: Arc<Store>,
    _guard: RwLock<()>,
}

impl ProviderImpl {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            _guard: RwLock::new(()),
        }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn get_key_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        value: &str,
    ) -> Result<u64> {
        // TODO investigate deadlock
        // let a = self._guard.write().await;
        match self
            .store
            .get(make_value_key(organization_id, project_id, dict, value))
            .await?
        {
            None => {
                let id = self
                    .store
                    .next_seq(make_id_seq_key(
                        org_proj_ns(organization_id, project_id, dict_ns(dict).as_slice())
                            .as_slice(),
                    ))
                    .await?;
                self.store
                    .put(
                        make_key_key(organization_id, project_id, dict, id),
                        value.as_bytes(),
                    )
                    .await?;
                self.store
                    .put(
                        make_value_key(organization_id, project_id, dict, value),
                        id.to_le_bytes().as_ref(),
                    )
                    .await?;

                Ok(id)
            }
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice())),
        }
    }

    async fn get_value(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        key: u64,
    ) -> Result<String> {
        let store_key = make_key_key(organization_id, project_id, dict, key);
        match self.store.get(store_key.as_slice()).await? {
            None => Err(DictionaryError::KeyNotFound(DictionaryKey::new(
                organization_id,
                project_id,
                dict.to_string(),
                key,
            ))
            .into()),
            Some(value) => Ok(String::from_utf8(value)?),
        }
    }

    async fn get_key(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        value: &str,
    ) -> Result<u64> {
        let store_key = make_value_key(organization_id, project_id, dict, value);
        match self.store.get(store_key.as_slice()).await? {
            None => Err(DictionaryError::ValueNotFound(DictionaryValue::new(
                organization_id,
                project_id,
                dict.to_string(),
                value.to_string(),
            ))
            .into()),
            Some(key) => Ok(LittleEndian::read_u64(key.as_slice())),
        }
    }
}

#[derive(Debug)]
pub struct SingleDictionaryProvider {
    organization_id: u64,
    project_id: u64,
    dict: String,
    provider: Arc<dyn Provider>,
}

impl Hash for SingleDictionaryProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.organization_id.hash(state);
        self.project_id.hash(state);
        self.dict.hash(state);
    }
}

impl SingleDictionaryProvider {
    pub fn new(
        organization_id: u64,
        project_id: u64,
        dict: String,
        provider: Arc<dyn Provider>,
    ) -> Self {
        Self {
            organization_id,
            project_id,
            dict,
            provider,
        }
    }

    pub async fn get_key_or_create(&self, value: &str) -> Result<u64> {
        self.provider
            .get_key_or_create(
                self.organization_id,
                self.project_id,
                self.dict.as_str(),
                value,
            )
            .await
    }

    pub async fn get_value(&self, key: u64) -> Result<String> {
        self.provider
            .get_value(
                self.organization_id,
                self.project_id,
                self.dict.as_str(),
                key,
            )
            .await
    }

    pub async fn get_key(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        value: &str,
    ) -> Result<u64> {
        self.provider
            .get_key(
                self.organization_id,
                self.project_id,
                self.dict.as_str(),
                value,
            )
            .await
    }
}
