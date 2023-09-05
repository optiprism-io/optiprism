use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::custom_properties::CustomProperty;
use crate::custom_properties::Provider;
use crate::error;
use crate::error::MetadataError;
use crate::error::PropertyError;
use crate::error::StoreError;
use crate::metadata::ListResponse;
use crate::properties::CreatePropertyRequest;
use crate::properties::UpdatePropertyRequest;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_proj_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"custom_events";

pub struct ProviderImpl {
    store: Arc<Store>,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>) -> Self {
        Self { store: kv }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn list(
        &self,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<CustomProperty>> {
        list(
            self.store.clone(),
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
        .await
    }
}
