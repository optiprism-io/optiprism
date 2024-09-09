use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use metadata::bookmarks::Bookmarks as MDBookmarks;
use serde::Deserialize;
use serde::Serialize;

use crate::{Context, Result};
use crate::reports::Query;

pub struct Bookmarks {
    prov: Arc<MDBookmarks>,
}

impl Bookmarks {
    pub fn new(prov: Arc<MDBookmarks>) -> Self {
        Self { prov }
    }
    pub async fn create(
        &self,
        ctx: Context,
        project_id: u64,
        request: CreateBookmarkRequest,
    ) -> Result<Bookmark> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        let bookmark = self
            .prov
            .create(project_id, metadata::bookmarks::CreateBookmarkRequest {
                created_by: ctx.account_id,
                query: request.query.map(|q|q.into()),
            })?;

        Ok(bookmark.into())
    }

    pub async fn get(&self, ctx: Context, project_id: u64, id: &str) -> Result<Bookmark> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        Ok(self.prov.get_by_id(project_id, ctx.account_id, id)?.into())
    }
}

impl From<metadata::bookmarks::Bookmark> for Bookmark {
    fn from(value: metadata::bookmarks::Bookmark) -> Self {
        Bookmark {
            id: value.id,
            created_at: value.created_at,
            created_by: value.created_by,
            project_id: value.project_id,
            query: value.query.map(|q|q.into()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Bookmark {
    pub id: String,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub project_id: u64,
    pub query: Option<Query>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateBookmarkRequest {
    pub query: Option<Query>,
}