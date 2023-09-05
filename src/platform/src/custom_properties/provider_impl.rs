use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;

use crate::custom_properties;
use crate::custom_properties::CustomProperty;
use crate::custom_properties::Provider;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<dyn metadata::custom_properties::Provider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<dyn metadata::custom_properties::Provider>) -> Self {
        Self { prov }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<CustomProperty>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(organization_id, project_id).await?;

        resp.try_into()
    }
}
