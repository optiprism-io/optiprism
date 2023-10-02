use std::sync::Arc;

use async_trait::async_trait;






use crate::custom_properties::CustomProperty;
use crate::custom_properties::Provider;




use crate::metadata::ListResponse;



use crate::store::path_helpers::list;



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
