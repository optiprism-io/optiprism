use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::config::Config;
use common::rbac::OrganizationPermission;
use common::types::DType;
use common::types::OptionalProperty;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_PROPERTY_A_CLASS;
use common::types::EVENT_PROPERTY_A_HREF;
use common::types::EVENT_PROPERTY_A_ID;
use common::types::EVENT_PROPERTY_A_NAME;
use common::types::EVENT_PROPERTY_A_STYLE;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_REFERER;
use common::types::EVENT_PROPERTY_PAGE_SEARCH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::EVENT_PROPERTY_SESSION_LENGTH;
use common::types::EVENT_SCREEN;
use common::types::EVENT_SESSION_BEGIN;
use common::types::EVENT_SESSION_END;
use common::types::USER_PROPERTY_CITY;
use common::types::USER_PROPERTY_CLIENT_FAMILY;
use common::types::USER_PROPERTY_CLIENT_VERSION_MAJOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_MINOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_PATCH;
use common::types::USER_PROPERTY_COUNTRY;
use common::types::USER_PROPERTY_DEVICE_BRAND;
use common::types::USER_PROPERTY_DEVICE_FAMILY;
use common::types::USER_PROPERTY_DEVICE_MODEL;
use common::types::USER_PROPERTY_OS;
use common::types::USER_PROPERTY_OS_FAMILY;
use common::types::USER_PROPERTY_OS_VERSION_MAJOR;
use common::types::USER_PROPERTY_OS_VERSION_MINOR;
use common::types::USER_PROPERTY_OS_VERSION_PATCH;
use common::types::USER_PROPERTY_OS_VERSION_PATCH_MINOR;
use metadata::projects::Projects as MDProjects;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::util::create_event;
use metadata::util::create_property;
use metadata::util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::error;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct Projects {
    md: Arc<MetadataProvider>,
    cfg: Config,
}

impl Projects {
    pub fn new(prov: Arc<MetadataProvider>, cfg: Config) -> Self {
        Self { md: prov, cfg }
    }
    pub async fn create(&self, ctx: Context, request: CreateProjectRequest) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ManageProjects)?;

        let md = metadata::projects::CreateProjectRequest {
            created_by: ctx.account_id.unwrap(),
            tags: request.tags,
            name: request.name,
            description: request.description,
            session_duration_seconds: request
                .session_duration_seconds
                .unwrap_or(self.cfg.project_default_session_duration.num_seconds() as u64),
            organization_id: ctx.organization_id,
        };

        let project = self.md.projects.create(md)?;
        init_project(project.id, &self.md)?;
        Ok(project.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ExploreProjects)?;

        Ok(self.md.projects.get_by_id(id)?.into())
    }

    pub async fn list(
        &self,
        ctx: Context,
        organization_id: Option<u64>,
    ) -> Result<ListResponse<Project>> {
        ctx.check_organization_permission(OrganizationPermission::ExploreProjects)?;
        let resp = self.md.projects.list(organization_id)?;

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
            session_duration_seconds: req.session_duration_seconds,
        };

        let project = self.md.projects.update(project_id, md_req)?;

        Ok(project.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64) -> Result<Project> {
        ctx.check_organization_permission(OrganizationPermission::ManageProjects)?;

        Ok(self.md.projects.delete(project_id)?.into())
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
    pub session_duration_seconds: u64,
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
            session_duration_seconds: value.session_duration_seconds,
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
    pub session_duration_seconds: Option<u64>,
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
    pub session_duration_seconds: OptionalProperty<u64>,
}

pub fn init_project(project_id: u64, md: &Arc<MetadataProvider>) -> error::Result<()> {
    create_event(md, project_id, EVENT_CLICK.to_string())?;
    create_event(md, project_id, EVENT_PAGE.to_string())?;
    create_event(md, project_id, EVENT_SCREEN.to_string())?;
    create_event(md, project_id, EVENT_SESSION_BEGIN.to_string())?;
    create_event(md, project_id, EVENT_SESSION_END.to_string())?;
    let user_dict_props = vec![
        USER_PROPERTY_CLIENT_FAMILY,
        USER_PROPERTY_CLIENT_VERSION_MINOR,
        USER_PROPERTY_CLIENT_VERSION_MAJOR,
        USER_PROPERTY_CLIENT_VERSION_PATCH,
        USER_PROPERTY_DEVICE_FAMILY,
        USER_PROPERTY_DEVICE_BRAND,
        USER_PROPERTY_DEVICE_MODEL,
        USER_PROPERTY_OS,
        USER_PROPERTY_OS_FAMILY,
        USER_PROPERTY_OS_VERSION_MAJOR,
        USER_PROPERTY_OS_VERSION_MINOR,
        USER_PROPERTY_OS_VERSION_PATCH,
        USER_PROPERTY_OS_VERSION_PATCH_MINOR,
        USER_PROPERTY_COUNTRY,
        USER_PROPERTY_CITY,
    ];
    for prop in user_dict_props {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int64),
        })?;
    }

    let event_str_props = vec![
        EVENT_PROPERTY_A_NAME,
        EVENT_PROPERTY_A_HREF,
        EVENT_PROPERTY_A_ID,
        EVENT_PROPERTY_A_CLASS,
        EVENT_PROPERTY_A_STYLE,
        EVENT_PROPERTY_PAGE_PATH,
        EVENT_PROPERTY_PAGE_REFERER,
        EVENT_PROPERTY_PAGE_SEARCH,
        EVENT_PROPERTY_PAGE_TITLE,
        EVENT_PROPERTY_PAGE_URL,
    ];

    for prop in event_str_props {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: None,
        })?;
    }

    create_property(md, project_id, CreatePropertyMainRequest {
        name: EVENT_PROPERTY_SESSION_LENGTH.to_string(),
        typ: Type::Event,
        data_type: DType::Timestamp,
        nullable: true,
        dict: None,
    })?;

    Ok(())
}