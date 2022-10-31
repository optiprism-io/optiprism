use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use metadata::events::Provider as EventsProvider;

use crate::events::CreateEventRequest;
use crate::events::Event;
use crate::events::Provider;
use crate::events::UpdateEventRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<EventsProvider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<EventsProvider>) -> Self {
        Self { prov }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        request: CreateEventRequest,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let event = self
            .prov
            .create(
                organization_id,
                project_id,
                metadata::events::CreateEventRequest {
                    created_by: ctx.account_id.unwrap(),
                    tags: request.tags,
                    name: request.name,
                    display_name: request.display_name,
                    description: request.description,
                    status: request.status.into(),
                    is_system: request.is_system,
                    properties: None,
                    custom_properties: None,
                },
            )
            .await?;

        event.try_into()
    }

    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        self.prov
            .get_by_id(organization_id, project_id, id)
            .await?
            .try_into()
    }

    async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        let event = self
            .prov
            .get_by_name(organization_id, project_id, name)
            .await?;

        event.try_into()
    }

    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Event>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(organization_id, project_id).await?;

        resp.try_into()
    }

    async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateEventRequest,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let md_req = metadata::events::UpdateEventRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            display_name: req.display_name,
            description: req.description,
            status: req.status.into(),
            ..Default::default()
        };

        let event = self
            .prov
            .update(organization_id, project_id, event_id, md_req)
            .await?;

        event.try_into()
    }

    async fn attach_property(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        self.prov
            .attach_property(organization_id, project_id, event_id, prop_id)
            .await?
            .try_into()
    }

    async fn detach_property(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        self.prov
            .detach_property(organization_id, project_id, event_id, prop_id)
            .await?
            .try_into()
    }

    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        self.prov
            .delete(organization_id, project_id, id)
            .await?
            .try_into()
    }
}
