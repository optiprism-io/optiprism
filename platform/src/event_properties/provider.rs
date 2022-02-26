use crate::{Context, Result};
use common::rbac::Permission;
use metadata::event_properties::EventProperty;
use metadata::metadata::ListResponse;
use metadata::Metadata;
use std::sync::Arc;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<EventProperty> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::GetEventPropertyById,
        )?;
        Ok(self
            .metadata
            .event_properties
            .get_by_id(ctx.organization_id, project_id, id)
            .await?)
    }

    pub async fn get_by_name(
        &self,
        ctx: Context,
        project_id: u64,
        name: &str,
    ) -> Result<EventProperty> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::GetEventByName)?;
        let event = self
            .metadata
            .event_properties
            .get_by_name(ctx.organization_id, project_id, name)
            .await?;
        Ok(event)
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<EventProperty>> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::ListEventProperties,
        )?;
        Ok(self
            .metadata
            .event_properties
            .list(ctx.organization_id, project_id)
            .await?)
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<EventProperty> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::DeleteEventProperty,
        )?;
        Ok(self
            .metadata
            .event_properties
            .delete(ctx.organization_id, project_id, id)
            .await?)
    }
}
