use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::custom_events::CustomEvents as MDCustomEvents;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::EventRef;
use crate::ListResponse;
use crate::PlatformError;
use crate::PropValueFilter;
use crate::Result;

pub struct CustomEvents {
    prov: Arc<MDCustomEvents>,
}

impl CustomEvents {
    pub fn new(prov: Arc<MDCustomEvents>) -> Self {
        Self { prov }
    }

    pub async fn create(
        &self,
        ctx: Context,

        project_id: u64,
        req: CreateCustomEventRequest,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(project_id, ProjectPermission::ManageSchema)?;

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
                .map(|e| e.to_owned().into())
                .collect::<Vec<_>>(),
        };

        let event = self.prov.create(project_id, md_req)?;

        Ok(event.into())
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<CustomEvent> {
        ctx.check_project_permission(project_id, ProjectPermission::ViewSchema)?;
        Ok(self.prov.get_by_id(project_id, id)?.into())
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<CustomEvent>> {
        ctx.check_project_permission(project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(project_id)?;

        Ok(resp.into())
    }

    pub async fn update(
        &self,
        ctx: Context,

        project_id: u64,
        event_id: u64,
        req: UpdateCustomEventRequest,
    ) -> Result<CustomEvent> {
        ctx.check_project_permission(project_id, ProjectPermission::ManageSchema)?;
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
                    .map(|e| e.to_owned().into())
                    .collect::<Vec<_>>(),
            );
        }
        let event = self.prov.update(project_id, event_id, md_req)?;

        Ok(event.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<CustomEvent> {
        ctx.check_project_permission(project_id, ProjectPermission::DeleteSchema)?;

        Ok(self.prov.delete(project_id, id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enabled,
    Disabled,
}

impl From<metadata::custom_events::Status> for Status {
    fn from(s: metadata::custom_events::Status) -> Self {
        match s {
            metadata::custom_events::Status::Enabled => Status::Enabled,
            metadata::custom_events::Status::Disabled => Status::Disabled,
        }
    }
}

impl From<Status> for metadata::custom_events::Status {
    fn from(s: Status) -> Self {
        match s {
            Status::Enabled => metadata::custom_events::Status::Enabled,
            Status::Disabled => metadata::custom_events::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}

impl Into<metadata::custom_events::Event> for Event {
    fn into(self) -> metadata::custom_events::Event {
        metadata::custom_events::Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|v| v.iter().map(|e| e.to_owned().into()).collect()),
        }
    }
}

impl Into<Event> for metadata::custom_events::Event {
    fn into(self) -> Event {
        Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|v| v.iter().map(|e| e.to_owned().into()).collect()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CustomEvent {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

impl Into<CustomEvent> for metadata::custom_events::CustomEvent {
    fn into(self) -> CustomEvent {
        CustomEvent {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            events: self
                .events
                .iter()
                .map(|e| e.to_owned().into())
                .collect::<Vec<_>>(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateCustomEventRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpdateCustomEventRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    // TODO move to container macro
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub status: OptionalProperty<Status>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub events: OptionalProperty<Vec<Event>>,
}
