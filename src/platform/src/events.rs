use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::events::Events as MDEvents;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

pub struct Events {
    prov: Arc<MDEvents>,
}

impl Events {
    pub fn new(prov: Arc<MDEvents>) -> Self {
        Self { prov }
    }
    pub async fn create(
        &self,
        ctx: Context,

        project_id: u64,
        request: CreateEventRequest,
    ) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ManageSchema)?;

        let event = self
            .prov
            .create(project_id, metadata::events::CreateEventRequest {
                created_by: ctx.account_id.unwrap(),
                tags: request.tags,
                name: request.name,
                display_name: request.display_name,
                description: request.description,
                status: request.status.into(),
                is_system: request.is_system,
                event_properties: None,
                user_properties: None,
                custom_properties: None,
            })?;

        event.try_into()
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ViewSchema)?;

        self.prov.get_by_id(project_id, id)?.try_into()
    }

    pub async fn get_by_name(&self, ctx: Context, project_id: u64, name: &str) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ViewSchema)?;

        let event = self.prov.get_by_name(project_id, name)?;

        event.try_into()
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Event>> {
        ctx.check_project_permission(ProjectPermission::ViewSchema)?;
        let resp = self.prov.list(project_id)?;

        resp.try_into()
    }

    pub async fn update(
        &self,
        ctx: Context,

        project_id: u64,
        event_id: u64,
        req: UpdateEventRequest,
    ) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ManageSchema)?;

        let md_req = metadata::events::UpdateEventRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            display_name: req.display_name,
            description: req.description,
            status: req.status.into(),
            ..Default::default()
        };

        let event = self.prov.update(project_id, event_id, md_req)?;

        event.try_into()
    }

    pub async fn attach_property(
        &self,
        ctx: Context,

        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ManageSchema)?;

        self.prov
            .attach_event_property(project_id, event_id, prop_id)?
            .try_into()
    }

    pub async fn detach_property(
        &self,
        ctx: Context,

        project_id: u64,
        event_id: u64,
        prop_id: u64,
    ) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::ManageSchema)?;

        self.prov
            .detach_event_property(project_id, event_id, prop_id)?
            .try_into()
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Event> {
        ctx.check_project_permission(ProjectPermission::DeleteSchema)?;

        self.prov.delete(project_id, id)?.try_into()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Enabled,
    Disabled,
}

impl From<metadata::events::Status> for Status {
    fn from(s: metadata::events::Status) -> Self {
        match s {
            metadata::events::Status::Enabled => Status::Enabled,
            metadata::events::Status::Disabled => Status::Disabled,
        }
    }
}

impl From<Status> for metadata::events::Status {
    fn from(s: Status) -> Self {
        match s {
            Status::Enabled => metadata::events::Status::Enabled,
            Status::Disabled => metadata::events::Status::Disabled,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
    pub event_properties: Option<Vec<u64>>,
    pub user_properties: Option<Vec<u64>>,
}

impl TryInto<metadata::events::Event> for Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<metadata::events::Event, Self::Error> {
        Ok(metadata::events::Event {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            event_properties: None,
            custom_properties: self.user_properties,
            user_properties: None,
        })
    }
}

impl TryInto<Event> for metadata::events::Event {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<Event, Self::Error> {
        Ok(Event {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status.into(),
            is_system: self.is_system,
            event_properties: self.event_properties,
            user_properties: self.custom_properties,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateEventRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub is_system: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateEventRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub display_name: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub status: OptionalProperty<Status>,
}
