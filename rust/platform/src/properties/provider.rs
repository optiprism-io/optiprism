use crate::properties::UpdatePropertyRequest;
use crate::{Context, Result};
use common::rbac::Permission;
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
        let perm = match self.ns {
            Namespace::Event => Permission::GetEventPropertyById,
            Namespace::User => Permission::GetUserPropertyById,
        };

        ctx.check_permission(organization_id, project_id, perm)?;

        Ok(self.prov.get_by_id(organization_id, project_id, id).await?)
    }

    pub async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Property> {
        ctx.check_permission(organization_id, project_id, Permission::GetEventByName)?;
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
        ctx.check_permission(organization_id, project_id, Permission::ListEventProperties)?;
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
        ctx.check_permission(organization_id, project_id, Permission::UpdateEvent)?;
        let mut md_req = metadata::properties::UpdatePropertyRequest::default();
        let _ = md_req.updated_by = ctx.account_id;
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
        ctx.check_permission(organization_id, project_id, Permission::DeleteEventProperty)?;
        Ok(self.prov.delete(organization_id, project_id, id).await?)
    }
}