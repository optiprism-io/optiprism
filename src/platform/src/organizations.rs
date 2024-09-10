use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::OrganizationPermission;
use common::rbac::Permission;
use common::types::OptionalProperty;
use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::Result;

pub struct Organizations {
    md: Arc<MetadataProvider>,
}

impl Organizations {
    pub fn new(prov: Arc<MetadataProvider>) -> Self {
        Self { md: prov }
    }
    pub async fn create(
        &self,
        ctx: Context,
        request: CreateOrganizationRequest,
    ) -> Result<Organization> {
        ctx.check_permission(Permission::ManageOrganizations)?;
        let md = metadata::organizations::CreateOrganizationRequest {
            created_by: ctx.account_id,
            name: request.name,
        };

        let org = self.md.organizations.create(md)?;
        Ok(org.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Organization> {
        ctx.check_organization_permission(id, OrganizationPermission::ViewOrganization)?;

        Ok(self.md.organizations.get_by_id(id)?.into())
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Organization>> {
        ctx.check_permission(Permission::ViewOrganizations)?;
        let resp = self.md.organizations.list()?;

        let list = resp
            .data
            .into_iter()
            .filter(|o| {
                ctx.check_organization_permission(o.id, OrganizationPermission::ViewOrganization)
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
        org_id: u64,
        req: UpdateOrganizationRequest,
    ) -> Result<Organization> {
        ctx.check_organization_permission(org_id, OrganizationPermission::ManageOrganization)?;

        let md_req = metadata::organizations::UpdateOrganizationRequest {
            updated_by: ctx.account_id,
            name: req.name,
        };

        let org = self.md.organizations.update(org_id, md_req)?;

        Ok(org.into())
    }

    pub async fn delete(&self, ctx: Context, org_id: u64) -> Result<Organization> {
        ctx.check_permission(Permission::ManageOrganizations)?;

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
