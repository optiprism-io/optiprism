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

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Property> {
        let perm = match self.ns {
            Namespace::Event => Permission::GetEventPropertyById,
            Namespace::User => Permission::GetUserPropertyById,
        };

        ctx.check_permission(ctx.organization_id, project_id, perm)?;

        Ok(self
            .prov
            .get_by_id(ctx.organization_id, project_id, id)
            .await?)
    }

    pub async fn get_by_name(&self, ctx: Context, project_id: u64, name: &str) -> Result<Property> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::GetEventByName)?;
        let event = self
            .prov
            .get_by_name(ctx.organization_id, project_id, name)
            .await?;
        Ok(event)
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Property>> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::ListEventProperties,
        )?;
        Ok(self.prov.list(ctx.organization_id, project_id).await?)
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Property> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::DeleteEventProperty,
        )?;
        Ok(self
            .prov
            .delete(ctx.organization_id, project_id, id)
            .await?)
    }
}
