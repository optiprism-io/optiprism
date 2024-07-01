use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::config::Config;
use common::rbac::OrganizationPermission;
use common::rbac::Permission;
use common::rbac::ProjectPermission;
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, COLUMN_EVENT_ID, COLUMN_IP, COLUMN_PROJECT_ID, DType, GROUP_COLUMN_CREATED_AT, GROUP_COLUMN_ID, GROUP_COLUMN_PROJECT_ID, GROUP_COLUMN_VERSION, TABLE_EVENTS};
use common::types::OptionalProperty;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_PROPERTY_CITY;
use common::types::EVENT_PROPERTY_CLASS;
use common::types::EVENT_PROPERTY_CLIENT_FAMILY;
use common::types::EVENT_PROPERTY_CLIENT_VERSION_MAJOR;
use common::types::EVENT_PROPERTY_CLIENT_VERSION_MINOR;
use common::types::EVENT_PROPERTY_CLIENT_VERSION_PATCH;
use common::types::EVENT_PROPERTY_COUNTRY;
use common::types::EVENT_PROPERTY_DEVICE_BRAND;
use common::types::EVENT_PROPERTY_DEVICE_FAMILY;
use common::types::EVENT_PROPERTY_DEVICE_MODEL;
use common::types::EVENT_PROPERTY_ELEMENT;
use common::types::EVENT_PROPERTY_HREF;
use common::types::EVENT_PROPERTY_ID;
use common::types::EVENT_PROPERTY_NAME;
use common::types::EVENT_PROPERTY_OS;
use common::types::EVENT_PROPERTY_OS_FAMILY;
use common::types::EVENT_PROPERTY_OS_VERSION_MAJOR;
use common::types::EVENT_PROPERTY_OS_VERSION_MINOR;
use common::types::EVENT_PROPERTY_OS_VERSION_PATCH;
use common::types::EVENT_PROPERTY_OS_VERSION_PATCH_MINOR;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_REFERER;
use common::types::EVENT_PROPERTY_PAGE_SEARCH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::EVENT_PROPERTY_SESSION_LENGTH;
use common::types::EVENT_PROPERTY_TEXT;
use common::types::EVENT_PROPERTY_UTM_CAMPAIGN;
use common::types::EVENT_PROPERTY_UTM_CONTENT;
use common::types::EVENT_PROPERTY_UTM_MEDIUM;
use common::types::EVENT_PROPERTY_UTM_SOURCE;
use common::types::EVENT_PROPERTY_UTM_TERM;
use common::types::EVENT_SCREEN;
use common::types::EVENT_SESSION_BEGIN;
use common::types::EVENT_SESSION_END;
use common::{group_col, GROUP_USER, GROUPS_COUNT};
use common::GROUP_USER_ID;
use metadata::projects::Projects as MDProjects;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::util::create_event;
use metadata::util::create_property;
use metadata::util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::thread_rng;
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
        ctx.check_organization_permission(
            ctx.organization_id,
            OrganizationPermission::ManageProjects,
        )?;

        let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

        let md = metadata::projects::CreateProjectRequest {
            created_by: ctx.account_id,
            tags: request.tags,
            name: request.name,
            description: request.description,
            session_duration_seconds: request
                .session_duration_seconds
                .unwrap_or(self.cfg.project_default_session_duration.num_seconds() as u64),
            organization_id: ctx.organization_id,
            token,
        };

        let project = self.md.projects.create(md)?;
        self.md.dictionaries.create_key(project.id, TABLE_EVENTS, "project_id", project.id, project.name.as_str())?;
        for g in 0..GROUPS_COUNT {
            self.md.dictionaries.create_key(project.id, group_col(g).as_str(), "project_id", project.id, project.name.as_str())?;
        }

        init_project(project.id, &self.md)?;
        Ok(project.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Project> {
        ctx.check_project_permission(ctx.organization_id, id, ProjectPermission::ViewProject)?;

        Ok(self.md.projects.get_by_id(id)?.into())
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Project>> {
        ctx.check_organization_permission(
            ctx.organization_id,
            OrganizationPermission::ExploreProjects,
        )?;
        let resp = self.md.projects.list()?;

        let list = resp
            .data
            .into_iter()
            .filter(|p| p.organization_id == ctx.organization_id)
            .collect::<Vec<_>>();

        let list = list
            .into_iter()
            .filter(|p| {
                ctx.check_project_permission(
                    ctx.organization_id,
                    p.id,
                    ProjectPermission::ViewProject,
                )
                    .is_ok()
            })
            .collect::<Vec<_>>();
        Ok(ListResponse {
            data: list.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    pub async fn update(
        &self,
        ctx: Context,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageProject,
        )?;

        let md_req = metadata::projects::UpdateProjectRequest {
            updated_by: ctx.account_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            session_duration_seconds: req.session_duration_seconds,
            token: req.token,
        };

        let project = self.md.projects.update(project_id, md_req)?;
        self.md.dictionaries.create_key(project.id, TABLE_EVENTS, "project_id", project.id, project.name.as_str())?;
        for g in 0..GROUPS_COUNT {
            self.md.dictionaries.create_key(project.id, group_col(g).as_str(), "project_id", project.id, project.name.as_str())?;
        }

        Ok(project.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64) -> Result<Project> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::DeleteProject,
        )?;

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
    pub sdk_token: String,
    pub name: String,
    pub description: Option<String>,
    pub session_duration_seconds: u64,
    pub events_count: usize,
}

impl From<metadata::projects::Project> for Project {
    fn from(value: metadata::projects::Project) -> Self {
        Project {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            tags: value.tags,
            sdk_token: value.token,
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
    pub token: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub session_duration_seconds: OptionalProperty<u64>,
}

pub fn init_project(project_id: u64, md: &Arc<MetadataProvider>) -> error::Result<()> {
    create_property(md, project_id, CreatePropertyMainRequest {
        name: COLUMN_PROJECT_ID.to_string(),
        display_name: Some("Project".to_string()),
        typ: Type::Event,
        data_type: DType::String,
        nullable: false,
        hidden: true,
        dict: Some(DictionaryType::Int64),
        is_system: true,
    })?;


    for g in 0..GROUPS_COUNT {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: group_col(g),
            display_name: Some(format!("Group {g}")),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            hidden: false,
            dict: Some(DictionaryType::Int64),
            is_system: true,
        })?;
    }
    create_property(md, project_id, CreatePropertyMainRequest {
        name: COLUMN_CREATED_AT.to_string(),
        display_name: Some("Created At".to_string()),
        typ: Type::Event,
        data_type: DType::Timestamp,
        nullable: false,
        hidden: false,
        dict: None,
        is_system: true,
    })?;

    create_property(md, project_id, CreatePropertyMainRequest {
        name: COLUMN_EVENT_ID.to_string(),
        display_name: Some("Event ID".to_string()),
        typ: Type::Event,
        data_type: DType::Int64,
        nullable: false,
        hidden: true,
        dict: None,
        is_system: true,
    })?;

    create_property(md, project_id, CreatePropertyMainRequest {
        name: COLUMN_EVENT.to_string(),
        display_name: Some("Event".to_string()),
        typ: Type::Event,
        data_type: DType::String,
        nullable: false,
        dict: Some(DictionaryType::Int64),
        hidden: true,
        is_system: true,
    })?;

    create_property(md, project_id, CreatePropertyMainRequest {
        name: COLUMN_IP.to_string(),
        display_name: Some("Ip".to_string()),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: None,
        hidden: true,
        is_system: true,
    })?;

    create_event(md, project_id, EVENT_CLICK.to_string())?;
    create_event(md, project_id, EVENT_PAGE.to_string())?;
    create_event(md, project_id, EVENT_SCREEN.to_string())?;
    create_event(md, project_id, EVENT_SESSION_BEGIN.to_string())?;
    create_event(md, project_id, EVENT_SESSION_END.to_string())?;
    let event_dict_props = vec![
        EVENT_PROPERTY_CLIENT_FAMILY,
        EVENT_PROPERTY_CLIENT_VERSION_MINOR,
        EVENT_PROPERTY_CLIENT_VERSION_MAJOR,
        EVENT_PROPERTY_CLIENT_VERSION_PATCH,
        EVENT_PROPERTY_DEVICE_FAMILY,
        EVENT_PROPERTY_DEVICE_BRAND,
        EVENT_PROPERTY_DEVICE_MODEL,
        EVENT_PROPERTY_OS,
        EVENT_PROPERTY_OS_FAMILY,
        EVENT_PROPERTY_OS_VERSION_MAJOR,
        EVENT_PROPERTY_OS_VERSION_MINOR,
        EVENT_PROPERTY_OS_VERSION_PATCH,
        EVENT_PROPERTY_OS_VERSION_PATCH_MINOR,
        EVENT_PROPERTY_COUNTRY,
        EVENT_PROPERTY_CITY,
        EVENT_PROPERTY_UTM_SOURCE,
        EVENT_PROPERTY_UTM_MEDIUM,
        EVENT_PROPERTY_UTM_CAMPAIGN,
        EVENT_PROPERTY_UTM_TERM,
        EVENT_PROPERTY_UTM_CONTENT,
    ];
    for prop in event_dict_props {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            display_name: None,
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            hidden: false,
            dict: Some(DictionaryType::Int64),
            is_system: false,
        })?;
    }

    let event_str_props = vec![
        EVENT_PROPERTY_NAME,
        EVENT_PROPERTY_HREF,
        EVENT_PROPERTY_ID,
        EVENT_PROPERTY_CLASS,
        EVENT_PROPERTY_TEXT,
        EVENT_PROPERTY_ELEMENT,
        EVENT_PROPERTY_PAGE_PATH,
        EVENT_PROPERTY_PAGE_REFERER,
        EVENT_PROPERTY_PAGE_SEARCH,
        EVENT_PROPERTY_PAGE_TITLE,
        EVENT_PROPERTY_PAGE_URL,
    ];

    for prop in event_str_props {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            display_name: None,
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            hidden: false,
            dict: None,
            is_system: false,
        })?;
    }

    create_property(md, project_id, CreatePropertyMainRequest {
        name: EVENT_PROPERTY_SESSION_LENGTH.to_string(),
        display_name: None,
        typ: Type::Event,
        data_type: DType::Timestamp,
        nullable: true,
        hidden: false,
        dict: None,
        is_system: false,
    })?;

    for g in 0..GROUPS_COUNT {
        create_property(md, project_id, CreatePropertyMainRequest {
            name: GROUP_COLUMN_PROJECT_ID.to_string(),
            display_name: Some("Project ID".to_string()),
            typ: Type::Group(g),
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int64),
            hidden: true,
            is_system: true,
        })?;

        create_property(md, project_id, CreatePropertyMainRequest {
            name: GROUP_COLUMN_ID.to_string(),
            display_name: Some("ID".to_string()),
            typ: Type::Group(g),
            data_type: DType::String,
            nullable: true, // todo make non-nullable
            dict: Some(DictionaryType::Int64),
            hidden: true,
            is_system: true,
        })?;

        create_property(md, project_id, CreatePropertyMainRequest {
            name: GROUP_COLUMN_VERSION.to_string(),
            display_name: Some("Version".to_string()),
            typ: Type::Group(g),
            data_type: DType::Int64,
            nullable: true,
            dict: None,
            hidden: true,
            is_system: true,
        })?;

        create_property(md, project_id, CreatePropertyMainRequest {
            name: GROUP_COLUMN_CREATED_AT.to_string(),
            display_name: Some("Created At".to_string()),
            typ: Type::Group(g),
            data_type: DType::Timestamp,
            nullable: true,
            hidden: false,
            dict: None,
            is_system: true,
        })?;
    }
    md.groups
        .get_or_create_group(project_id, GROUP_USER.to_string(), GROUP_USER.to_string())?;

    Ok(())
}
