use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;

use crate::properties::Property;
use crate::properties::Provider;
use crate::properties::UpdatePropertyRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<dyn metadata::properties::Provider>,
}

impl ProviderImpl {
    pub fn new_user(prov: Arc<dyn metadata::properties::Provider>) -> Self {
        Self { prov }
    }

    pub fn new_event(prov: Arc<dyn metadata::properties::Provider>) -> Self {
        Self { prov }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        self.prov
            .get_by_id(organization_id, project_id, id)?
            .try_into()
    }

    async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        let event = self.prov.get_by_name(organization_id, project_id, name)?;

        event.try_into()
    }

    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Property>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(organization_id, project_id)?;

        resp.try_into()
    }

    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        property_id: u64,
        req: UpdatePropertyRequest,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let md_req = metadata::properties::UpdatePropertyRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            description: req.description,
            display_name: req.display_name,
            status: req.status.into(),
            is_dictionary: Default::default(),
            dictionary_type: Default::default(),
            ..Default::default()
        };

        let prop = self
            .prov
            .update(organization_id, project_id, property_id, md_req)?;

        prop.try_into()
    }

    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        self.prov
            .delete(organization_id, project_id, id)?
            .try_into()
    }
}
