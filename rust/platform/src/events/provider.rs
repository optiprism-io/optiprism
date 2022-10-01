use crate::events::types::UpdateEventRequest;
use crate::events::CreateEventRequest;
use crate::{Context, Result};
use common::rbac::{Permission, ProjectPermission};
use metadata::events::{Event, Provider as EventsProvider};
use metadata::metadata::ListResponse;
use std::sync::Arc;

pub struct Provider {
    prov: Arc<EventsProvider>,
}

impl Provider {
    pub fn new(prov: Arc<EventsProvider>) -> Self {
        Self { prov }
    }

    pub async fn create(
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
                    status: request.status,
                    is_system: request.is_system,
                    properties: None,
                    custom_properties: None,
                },
            )
            .await?;

        Ok(event)
    }

    pub async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        Ok(self.prov.get_by_id(organization_id, project_id, id).await?)
    }

    pub async fn get_by_name(
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
        Ok(event)
    }

    pub async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<Event>> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;

        Ok(self.prov.list(organization_id, project_id).await?)
    }

    pub async fn update(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        req: UpdateEventRequest,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let mut md_req = metadata::events::UpdateEventRequest::default();
        let _ = md_req.updated_by = ctx.account_id.unwrap();
        let _ = md_req.tags.insert(req.tags);
        let _ = md_req.display_name.insert(req.display_name);
        let _ = md_req.description.insert(req.description);
        let _ = md_req.status.insert(req.status);
        let event = self
            .prov
            .update(organization_id, project_id, event_id, md_req)
            .await?;

        Ok(event)
    }

    pub async fn attach_property(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        Ok(self
            .prov
            .attach_property(organization_id, project_id, event_id, prop_id)
            .await?)
    }

    pub async fn detach_property(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        Ok(self
            .prov
            .detach_property(organization_id, project_id, event_id, prop_id)
            .await?)
    }

    pub async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        Ok(self.prov.delete(organization_id, project_id, id).await?)
    }
}
