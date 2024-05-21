use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use metadata::groups::Groups as MDGroups;
use serde::Deserialize;
use serde::Serialize;

use crate::rbac::ProjectPermission;
use crate::rbac::RBAC;
use crate::Context;
use crate::ListResponse;

pub struct Groups {
    prov: Arc<MDGroups>,
    rbac: Arc<RBAC>,
}

impl crate::groups::Groups {
    pub fn new(prov: Arc<MDGroups>, rbac: Arc<RBAC>) -> Self {
        Self { prov, rbac }
    }
    pub async fn create(
        &self,
        ctx: Context,
        project_id: u64,
        request: CreateGroupRequest,
    ) -> crate::Result<crate::groups::Group> {
        self.rbac.check_project_permission(
            project_id,
            ctx.account_id,
            ProjectPermission::ManageSchema,
        )?;

        let gid = self
            .prov
            .get_or_create_group_name(project_id, request.name.as_str())?;

        Ok(Group {
            id: gid,
            name: request.name,
        })
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> crate::Result<ListResponse<Group>> {
        self.rbac.check_project_permission(
            project_id,
            ctx.account_id,
            ProjectPermission::ViewSchema,
        )?;
        let resp = self.prov.list_names(project_id)?;

        let mut data = vec![];
        for (id, name) in resp.data.iter() {
            let g = Group {
                id: *id,
                name: name.clone(),
            };
            data.push(g);
        }
        Ok(ListResponse {
            data,
            meta: Default::default(),
        })
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Group {
    pub id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateGroupRequest {
    pub name: String,
}
