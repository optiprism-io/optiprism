use std::sync::Arc;

use axum::async_trait;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::custom_events;

use super::CreateCustomEventRequest;
use super::CustomEvent;
use super::Provider;
use crate::custom_events::UpdateCustomEventRequest;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct ProviderImpl {
    prov: Arc<dyn custom_events::Provider>,
}

impl ProviderImpl {
    pub fn new(prov: Arc<dyn custom_events::Provider>) -> Self {
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
        req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;

        let md_req = metadata::custom_events::CreateCustomEventRequest {
            created_by: ctx.account_id.unwrap(),
            tags: req.tags,
            name: req.name,
            description: req.description,
            status: req.status.into(),
            is_system: req.is_system,
            events: req
                .events
                .iter()
                .map(|e| e.to_owned().try_into())
                .collect::<Result<_>>()?,
        };

        let event = self
            .prov
            .create(organization_id, project_id, md_req)
            .await?;

        event.try_into()
    }

    async fn get_by_id(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ViewSchema)?;
        self.prov
            .get_by_id(organization_id, project_id, id)
            .await?
            .try_into()
    }

    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<CustomEvent>> {
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
        req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::ManageSchema)?;
        let mut md_req = metadata::custom_events::UpdateCustomEventRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            name: req.name,
            description: req.description,
            status: req.status.into(),
            ..Default::default()
        };

        if let OptionalProperty::Some(events) = req.events {
            md_req.events.insert(
                events
                    .iter()
                    .map(|e| e.to_owned().try_into())
                    .collect::<Result<_>>()?,
            );
        }
        let event = self
            .prov
            .update(organization_id, project_id, event_id, md_req)
            .await?;

        event.try_into()
    }

    async fn delete(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(organization_id, project_id, ProjectPermission::DeleteSchema)?;

        self.prov
            .delete(organization_id, project_id, id)
            .await?
            .try_into()
    }
}
