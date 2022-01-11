use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub struct Meta {
    expired_at: DateTime<Utc>,
}

pub struct KV {}

impl KV {
    async fn upsert(&self, key: &str, value: &[u8], meta: Option<Meta>) -> Result<()> {}
    async fn put(&self, key: &str, value: &[u8], meta: Option<Meta>) -> Result<()> {}
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {}
    async fn multi_get(&self, key: Vec<&str>) -> Result<Vec<Option<Vec<u8>>>> {}
    async fn delete(&self, key: &str) -> Result<()> {}
    async fn list(&self) -> Result<Vec<Vec<u8>>> {}

    async fn entity_insert_entity(&self, namespace: &str, value: &[u8], meta: Option<Meta>) -> Result<u64> {}
    async fn entity_update_entity(&self, namespace: &str, id: u64, value: &[u8], meta: Option<Meta>) -> Result<u64> {}
    async fn entity_put_entity(&self, namespace: &str, id: u64, value: &[u8], meta: Option<Meta>) -> Result<()> {}
    async fn entity_get_entity(&self, namespace: &str, id: u64) -> Result<Option<Vec<u8>>> {}
    async fn entity_multi_get_entity(&self, namespace: &str, id: Vec<u64>) -> Result<Vec<Option<Vec<u8>>>> {}
    async fn entity_delete(&self, namespace: &str, id: u64) -> Result<()> {}
}