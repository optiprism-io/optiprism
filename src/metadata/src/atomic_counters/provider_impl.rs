//! Atomic counters provider implementation with RocksDB storage.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use super::Provider;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"atomic_counters";
const ENTITY_EVENT_RECORD: &[u8] = b"event_record";

fn make_entity_counter_key(organization_id: u64, project_id: u64, entity: &[u8]) -> Vec<u8> {
    let mut counter_key = org_proj_ns(organization_id, project_id, NAMESPACE);
    counter_key.extend_from_slice(b"/");
    counter_key.extend_from_slice(entity);

    counter_key
}

pub struct ProviderImpl {
    store: Arc<Store>,
    lock: RwLock<()>, // TODO: investigate other ways for atomicity
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn next_event_record(&self, organization_id: u64, project_id: u64) -> Result<u64> {
        let _guard = self.lock.write().await;
        let key = make_entity_counter_key(organization_id, project_id, ENTITY_EVENT_RECORD);
        self.store.next_seq(key).await
    }
}
