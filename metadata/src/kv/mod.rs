use crate::error::Result;
use chrono::{DateTime, Utc};

pub struct Meta {
    expired_at: DateTime<Utc>,
}

pub struct KV {
    // namespace
}

impl KV {
    pub async fn put(&self, key: &[u8], value: &[u8], meta: Option<Meta>) -> Result<()> {
        // raft
        unimplemented!()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // direct read from current node
        unimplemented!()
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        // raft
        unimplemented!()
    }

    pub async fn batch_get(&self, keys: Vec<&[u8]>) -> Result<Vec<Option<Vec<u8>>>> {
        // direct read from current node
        unimplemented!()
    }

    pub async fn list(&self) -> Result<Vec<Vec<u8>>> {
        // direct read from current node
        unimplemented!()
    }

    pub async fn entity_insert_entity(
        &self,
        namespace: &str,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<u64> {
        unimplemented!()
    }

    pub async fn entity_update_entity(
        &self,
        namespace: &str,
        id: u64,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<u64> {
        unimplemented!()
    }

    pub async fn entity_put_entity(
        &self,
        namespace: &str,
        id: u64,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<()> {
        unimplemented!()
    }

    pub async fn entity_get_entity(&self, namespace: &str, id: u64) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub async fn entity_multi_get_entity(
        &self,
        namespace: &str,
        id: Vec<u64>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        unimplemented!()
    }

    pub async fn entity_delete(&self, namespace: &str, id: u64) -> Result<()> {
        unimplemented!()
    }
}
