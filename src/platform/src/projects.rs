use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::OrganizationPermission;
use common::types::OptionalProperty;
use metadata::projects::Projects as MDProjects;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct Projects {
    prov: Arc<MDProjects>,
}

impl Projects {
    pub fn new(prov: Arc<MDProjects>) -> Self {
        Self { prov }
    }
    pub async fn create(&self, ctx: Context, request: CreateProjectRequest) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ManageProjects)?;

        let md = metadata::projects::CreateProjectRequest {
            created_by: ctx.account_id.unwrap(),
            tags: request.tags,
            name: request.name,
            description: request.description,
            session_duration_seconds: request.session_duration,
            organization_id: ctx.organization_id,
        };

        let dashboard = self.prov.create(md)?;

        Ok(dashboard.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ExploreProjects)?;

        Ok(self.prov.get_by_id(id)?.into())
    }

    pub async fn list(
        &self,
        ctx: Context,
        organization_id: Option<u64>,
    ) -> Result<ListResponse<Project>> {
        ctx.check_organization_permission(OrganizationPermission::ExploreProjects)?;
        let resp = self.prov.list(organization_id)?;

        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    pub async fn update(
        &self,
        ctx: Context,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ManageProjects)?;

        let md_req = metadata::projects::UpdateProjectRequest {
            updated_by: ctx.account_id.unwrap(),
            tags: req.tags,
            name: req.name,
            description: req.description,
            session_duration_seconds: req.session_duration,
        };

        let project = self.prov.update(project_id, md_req)?;

        Ok(project.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ManageProjects)?;

        Ok(self.prov.delete(project_id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub session_duration: u64,
    pub events_count: usize,
}

impl From<metadata::projects::Project> for crate::projects::Project {
    fn from(value: metadata::projects::Project) -> Self {
        crate::projects::Project {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            tags: value.tags,
            name: value.name,
            description: value.description,
            session_duration: value.session_duration_seconds,
            events_count: value.events_count,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateProjectRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub session_duration: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProjectRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub session_duration: OptionalProperty<u64>,
}
