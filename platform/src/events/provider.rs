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
use metadata::events::{CreateEventRequest, Event, Scope, Status};

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn new(metadata: Arc<Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(&self, ctx: Context, request: CreateRequest) -> Result<Event> {
        ctx.check_action_permission(Permission::CreateEvent)?;
        ctx.maybe_check_project_permission(request.project_id)?;

        let event = self.metadata.events.create(CreateEventRequest {
            created_by: request.created_by,
            project_id: ctx.get_project_id(request.project_id),
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
        ctx.check_action_permission(Permission::GetEventById)?;
        ctx.check_project_permission(project_id)?;

        let event = self.metadata.events.get_by_id(project_id, id).await?;
        ctx.check_resource_permission(event.id, event.created_by, Resource::Event, Action::Read)?;
        Ok(event)
    }
}
