use crate::properties::UpdatePropertyRequest;
use crate::{Context, Result};
use common::rbac::{Permission, ProjectPermission};
use metadata::metadata::ListResponse;
use metadata::properties::provider::Namespace;
use metadata::properties::provider::Provider as PropertiesProvider;
use metadata::properties::Property;
use std::sync::Arc;

pub struct Provider {
    prov: Arc<PropertiesProvider>,
    ns: metadata::properties::provider::Namespace,
}

impl Provider {
    pub fn new_user(prov: Arc<PropertiesProvider>) -> Self {
        Self {
            prov,
            ns: Namespace::User,
        }
    }

    pub fn new_event(prov: Arc<PropertiesProvider>) -> Self {
        Self {
            prov,
            ns: Namespace::Event,
        }
    }

    pub async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        Ok(self.prov.get_by_id(organization_id, project_id, id).await?)
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
        Ok(event)
    }

    pub async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Property>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        Ok(self.prov.list(organization_id, project_id).await?)
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
        let _ = md_req.updated_by = ctx.account_id.unwrap();
        let _ = md_req.tags.insert(req.tags);
        let _ = md_req.display_name.insert(req.display_name);
        let _ = md_req.description.insert(req.description);
        let _ = md_req.status.insert(req.status);
        let prop = self
            .prov
            .update(organization_id, project_id, property_id, md_req)
            .await?;

        Ok(prop)
    }

    pub async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Property> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        Ok(self.prov.delete(organization_id, project_id, id).await?)
    }
}
