use std::intrinsics::forget;
use super::CreateRequest;
use crate::{Context, Result};
use common::{
    auth::{make_password_hash, make_salt},
    rbac::Permission,
};
use metadata::{
    accounts::{Account, CreateRequest as CreateAccountRequest},
    Metadata,
};
use std::sync::Arc;
use common::rbac::{Action, Resource};
use metadata::events::{CreateEventRequest, Event, Scope, Status, UpdateEventRequest};
use crate::events::types::UpdateRequest;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(&self, ctx: Context, project_id: u64, request: CreateRequest) -> Result<Event> {
        ctx.check_permission(request.organization_id, request.project_id, Permission::CreateAccount)?;

        let event = self.metadata.events.create(CreateEventRequest {
            created_by: request.created_by,
            project_id,
            tags: request.tags,
            name: request.name,
            display_name: request.display_name,
            description: request.description,
            status: request.status,
            scope: request.scope,
            properties: request.properties,
            global_properties: request.global_properties,
            custom_properties: request.custom_properties,
        }).await?;

        Ok(event)
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        ctx.check_permission(request.organization_id, project_id, Permission::GetEventById)?;

        let event = self.metadata.events.get_by_id(project_id, id).await?;
        ctx.check_ownership(event.created_by)?;
        Ok(event)
    }

    pub async fn get_by_name(&self, ctx: Context, project_id: u64, name: &str) -> Result<Event> {
        ctx.check_permission(request.organization_id, project_id, Permission::GetEventByName)?;

        let event = self.metadata.events.get_by_name(project_id, name).await?;
        ctx.check_ownership(event.created_by)?;
        Ok(event)
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<Vec<Event>> {
        ctx.check_permission(request.organization_id, project_id, Permission::ListEvents)?;

        let event = self.metadata.events.list().await?;
        Ok(event)
    }

    pub async fn update(&self, ctx: Context, project_id: u64, event_id: u64, req: UpdateRequest) -> Result<Event> {
        ctx.check_permission(request.organization_id, request.project_id, Permission::UpdateEvent)?;

        let event = self.metadata.events.update(UpdateEventRequest {
            id: event_id,
            created_by: req.created_by,
            updated_by: req.updated_by,
            project_id,
            tags: req.tags,
            name: req.name,
            display_name: req.display_name,
            description: req.description,
            status: req.status,
            scope: req.scope,
            properties: req.properties,
            global_properties: req.global_properties,
            custom_properties: req.custom_properties,
        }).await?;

        Ok(event)
    }

    pub async fn attach_property(&self, ctx: Context, project_id: u64, event_id: u64, prop_id: u64) -> Result<Event> {
        ctx.check_permission(request.organization_id, request.project_id, Permission::UpdateEvent)?;

        let mut event = self.metadata.events.get_by_id(project_id, event_id).await?;
        ctx.check_ownership(event.created_by)?;
        Ok(self.metadata.events.attach_property(project_id, event_id, prop_id).await?)
    }

    pub async fn detach_property(&self, ctx: Context, project_id: u64, event_id: u64, prop_id: u64) -> Result<Event> {
        ctx.check_permission(request.organization_id, request.project_id, Permission::UpdateEvent)?;

        let mut event = self.metadata.events.get_by_id(project_id, event_id).await?;
        ctx.check_ownership(event.created_by)?;
        Ok(self.metadata.events.detach_property(project_id, event_id, prop_id).await?)
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        let mut event = self.metadata.events.get_by_id(project_id, event_id).await?;
        ctx.check_ownership(event.created_by)?;
        Ok(self.metadata.events.delete(project_id, event_id).await?)
    }
}
