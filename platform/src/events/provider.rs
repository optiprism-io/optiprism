use super::CreateRequest;
use crate::events::types::UpdateRequest;
use crate::{Context, Result};
use common::rbac::Permission;
use metadata::events::{CreateEventRequest, Event, Provider as EventsProvider, UpdateEventRequest};
use metadata::metadata::ListResponse;
use std::sync::Arc;

pub struct Provider {
    prov: Arc<EventsProvider>,
}

impl Provider {
    pub fn new(prov: Arc<EventsProvider>) -> Self {
        Self { prov }
    }

    pub async fn create(&self, ctx: Context, request: CreateRequest) -> Result<Event> {
        ctx.check_permission(
            ctx.organization_id,
            request.project_id,
            Permission::CreateAccount,
        )?;

        let event = self
            .prov
            .create(
                ctx.organization_id,
                CreateEventRequest {
                    created_by: ctx.account_id,
                    project_id: request.project_id,
                    tags: request.tags,
                    name: request.name,
                    display_name: request.display_name,
                    description: request.description,
                    status: request.status,
                    scope: request.scope,
                    properties: request.properties,
                    custom_properties: request.custom_properties,
                },
            )
            .await?;

        Ok(event)
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::GetEventById)?;
        Ok(self
            .prov
            .get_by_id(ctx.organization_id, project_id, id)
            .await?)
    }

    pub async fn get_by_name(&self, ctx: Context, project_id: u64, name: &str) -> Result<Event> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::GetEventByName)?;
        let event = self
            .prov
            .get_by_name(ctx.organization_id, project_id, name)
            .await?;
        Ok(event)
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Event>> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::ListEvents)?;
        Ok(self.prov.list(ctx.organization_id, project_id).await?)
    }

    pub async fn update(&self, ctx: Context, req: UpdateRequest) -> Result<Event> {
        ctx.check_permission(ctx.organization_id, req.project_id, Permission::UpdateEvent)?;
        let event = self
            .prov
            .update(
                ctx.organization_id,
                UpdateEventRequest {
                    id: req.id,
                    updated_by: ctx.account_id,
                    project_id: req.project_id,
                    tags: req.tags,
                    name: req.name,
                    display_name: req.display_name,
                    description: req.description,
                    status: req.status,
                    scope: req.scope,
                    properties: req.properties,
                    custom_properties: req.custom_properties,
                },
            )
            .await?;

        Ok(event)
    }

    pub async fn attach_property(
        &self,
        ctx: Context,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::AttachPropertyToEvent,
        )?;
        Ok(self
            .prov
            .attach_property(ctx.organization_id, project_id, event_id, prop_id)
            .await?)
    }

    pub async fn detach_property(
        &self,
        ctx: Context,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_permission(
            ctx.organization_id,
            project_id,
            Permission::DetachPropertyFromEvent,
        )?;
        Ok(self
            .prov
            .detach_property(ctx.organization_id, project_id, event_id, prop_id)
            .await?)
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        ctx.check_permission(ctx.organization_id, project_id, Permission::DeleteEvent)?;
        Ok(self
            .prov
            .delete(ctx.organization_id, project_id, id)
            .await?)
    }
}