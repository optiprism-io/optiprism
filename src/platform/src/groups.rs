use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use metadata::groups::Groups as MDGroups;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;

pub struct Groups {
    prov: Arc<MDGroups>,
}

impl crate::groups::Groups {
    pub fn new(prov: Arc<MDGroups>) -> Self {
        Self { prov }
    }
    pub async fn create(
        &self,
        ctx: Context,
        project_id: u64,
        request: CreateGroupRequest,
    ) -> crate::Result<crate::groups::Group> {
        ctx.check_project_permission(project_id, ProjectPermission::ManageSchema)?;

        let g = self
            .prov
            .get_or_create_group(project_id, request.name, request.display_name)?;

        Ok(Group {
            id: g.id,
            name: g.name,
        })
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> crate::Result<ListResponse<Group>> {
        ctx.check_project_permission(project_id, ProjectPermission::ViewSchema)?;
        let resp = self.prov.list_groups(project_id)?;

        let mut data = vec![];
        for g in resp.data.iter() {
            let g = Group {
                id: g.id,
                name: g.name.clone(),
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
    pub display_name: String,
}
