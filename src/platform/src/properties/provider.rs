use std::sync::Arc;

use common::rbac::ProjectPermission;
use metadata::properties::provider::Provider as PropertiesProvider;

use crate::properties::types::Property;
use crate::properties::UpdatePropertyRequest;
use crate::types::ListResponse;
use crate::Context;
use crate::Result;

pub struct Provider {
    prov: Arc<PropertiesProvider>,
}

impl Provider {
    pub fn new_user(prov: Arc<PropertiesProvider>) -> Self {
        Self { prov }
    }

    pub fn new_event(prov: Arc<PropertiesProvider>) -> Self {
        Self { prov }
    }

    pub async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        self.prov
            .get_by_id(organization_id, project_id, id)
            .await?
            .try_into()
    }

    pub async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        let event = self
            .prov
            .get_by_name(organization_id, project_id, name)
            .await?;

        event.try_into()
    }

    pub async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Property>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(organization_id, project_id).await?;

        resp.try_into()
    }

    pub async fn update(
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
            .update(organization_id, project_id, property_id, md_req)
            .await?;

        prop.try_into()
    }

    pub async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        self.prov
            .delete(organization_id, project_id, id)
            .await?
            .try_into()
    }
}
