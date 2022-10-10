use crate::properties::UpdatePropertyRequest;
use crate::{Context, Result};
use common::rbac::ProjectPermission;
use metadata::metadata::ListResponse;

use metadata::properties::provider::Provider as PropertiesProvider;
use std::sync::Arc;
use crate::properties::types::Property;

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

        self.prov.get_by_id(organization_id, project_id, id).await?.try_into()
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

        Ok(ListResponse {
            data: resp
                .data
                .iter()
                .map(|v| v.to_owned().try_into())
                .collect::<Result<_>>()?,
            meta: resp.meta,
        })
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

        let mut md_req = metadata::properties::UpdatePropertyRequest::default();
        md_req.updated_by = ctx.account_id.unwrap();
        md_req.tags = req.tags;
        md_req.display_name = req.display_name;
        md_req.description = req.description;
        md_req.status = req.status.into();
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

        self.prov.delete(organization_id, project_id, id).await?.try_into()
    }
}
