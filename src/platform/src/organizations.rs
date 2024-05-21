use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::config::Config;
use common::types::DType;
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
use common::types::EVENT_SCREEN;
use common::types::EVENT_SESSION_BEGIN;
use common::types::EVENT_SESSION_END;
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
use crate::rbac::Permission;
use crate::rbac::RBAC;
use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct Organizations {
    md: Arc<MetadataProvider>,
    rbac: Arc<RBAC>,
    cfg: Config,
}

impl Organizations {
    pub fn new(prov: Arc<MetadataProvider>, rbac: Arc<RBAC>, cfg: Config) -> Self {
        Self {
            md: prov,
            rbac,
            cfg,
        }
    }
    pub async fn create(
        &self,
        ctx: Context,
        request: CreateOrganizationRequest,
    ) -> Result<Organization> {
        self.rbac
            .check_permission(ctx.account_id, Permission::ManageOrganizations)?;

        let md = metadata::organizations::CreateOrganizationRequest {
            created_by: ctx.account_id,
            name: request.name,
        };

        let org = self.md.organizations.create(md)?;
        Ok(org.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Organization> {
        self.rbac
            .check_permission(ctx.account_id, Permission::ViewOrganizations)?;

        Ok(self.md.organizations.get_by_id(id)?.into())
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Organization>> {
        self.rbac
            .check_permission(ctx.account_id, Permission::ViewOrganizations)?;
        let resp = self.md.organizations.list()?;

        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    pub async fn update(
        &self,
        ctx: Context,
        org_id: u64,
        req: UpdateOrganizationRequest,
    ) -> Result<Organization> {
        self.rbac
            .check_permission(ctx.account_id, Permission::ManageOrganizations)?;

        let md_req = metadata::organizations::UpdateOrganizationRequest {
            updated_by: ctx.account_id,
            name: req.name,
        };

        let org = self.md.organizations.update(org_id, md_req)?;

        Ok(org.into())
    }

    pub async fn delete(&self, ctx: Context, org_id: u64) -> Result<Organization> {
        self.rbac
            .check_permission(ctx.account_id, Permission::ManageOrganizations)?;

        Ok(self.md.organizations.delete(org_id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub name: String,
}

impl From<metadata::organizations::Organization> for crate::organizations::Organization {
    fn from(value: metadata::organizations::Organization) -> Self {
        crate::organizations::Organization {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            name: value.name,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrganizationRequest {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateOrganizationRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
}
