use crate::events::types::UpdateEventRequest;
use crate::events::CreateEventRequest;
use crate::{Context, Result};
use common::rbac::Permission;
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
        ctx.check_permission(organization_id, project_id, Permission::CreateEvent)?;

        let event = self
            .prov
            .create(
                organization_id,
                project_id,
                metadata::events::CreateEventRequest {
                    created_by: ctx.account_id,
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
        ctx.check_permission(organization_id, project_id, Permission::GetEventById)?;
        Ok(self.prov.get_by_id(organization_id, project_id, id).await?)
    }

    pub async fn get_by_name(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        name: &str,
    ) -> Result<Event> {
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
    ) -> Result<ListResponse<Event>> {
        ctx.check_permission(organization_id, project_id, Permission::ListEvents)?;
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
        ctx.check_permission(organization_id, project_id, Permission::UpdateEvent)?;
        let mut md_req = metadata::events::UpdateEventRequest::default();
        md_req.updated_by = ctx.account_id;
        md_req.tags.insert(req.tags);
        md_req.display_name.insert(req.display_name);
        md_req.description.insert(req.description);
        md_req.status.insert(req.status);
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
        ctx.check_permission(
            organization_id,
            project_id,
            Permission::AttachPropertyToEvent,
        )?;
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
        ctx.check_permission(
            organization_id,
            project_id,
            Permission::DetachPropertyFromEvent,
        )?;
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
        ctx.check_permission(organization_id, project_id, Permission::DeleteEvent)?;
        Ok(self.prov.delete(organization_id, project_id, id).await?)
    }
}
