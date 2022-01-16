pub mod types;
pub mod rbac;
pub mod kv;

use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub struct Meta {
    expired_at: DateTime<Utc>,
}

#[async_trait]
pub trait KV: Send + Sync {
    async fn upsert(&self, namespace: &str, key: &str, value: &[u8], meta: Option<Meta>) -> Result<()>;
    async fn put(&self, namespace: &str, key: &str, value: &[u8], meta: Option<Meta>) -> Result<()>;
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn multi_get(&self, namespace: &str, key: Vec<&str>) -> Result<Vec<Option<Vec<u8>>>>;
    async fn delete(&self, namespace: &str, key: &str) -> Result<()>;
    async fn list(&self, namespace: &str) -> Result<Vec<Vec<u8>>>;
}

#[async_trait]
pub trait SeqKV: Send + Sync {
    async fn insert(&self, value: &[u8], meta: Option<Meta>) -> Result<u64>;
    async fn update(&self, id: u64, value: &[u8], meta: Option<Meta>) -> Result<u64>;
    async fn put(&self, id: u64, value: &[u8], meta: Option<Meta>) -> Result<()>;
    async fn get(&self, id: u64) -> Result<Option<Vec<u8>>>;
    async fn multi_get(&self, id: Vec<u64>) -> Result<Vec<Option<Vec<u8>>>>;
    async fn delete(&self, id: u64) -> Result<()>;
    async fn list(&self) -> Result<Vec<Vec<u8>>>;
}
