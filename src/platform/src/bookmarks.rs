use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::bookmarks::Bookmarks as MDBookmarks;
use serde::Deserialize;
use serde::Serialize;

use crate::{Context, Result};
use crate::event_segmentation::EventSegmentationRequest;
use crate::funnel::FunnelRequest;

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
            ProjectPermission::ExploreBookmarks,
        )?;

        let bookmark = self
            .prov
            .create(project_id, metadata::bookmarks::CreateBookmarkRequest {
                created_by: ctx.account_id,
                name: request.name,
                typ: request.typ.into(),
                query: request.query.into(),
            })?;

        Ok(bookmark.into())
    }

    pub async fn get(&self, ctx: Context, project_id: u64, slug: u64) -> Result<Bookmark> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreBookmarks,
        )?;

        Ok(self.prov.get(project_id, slug)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum QueryType {
    EventSegmentation,
    Funnel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Query {
    EventSegmentation(EventSegmentationRequest),
    Funnel(FunnelRequest),
}

impl From<metadata::bookmarks::Query> for Query {
    fn from(value: metadata::bookmarks::Query) -> Self {
        match value {
            metadata::bookmarks::Query::EventSegmentation(es) => {
                Query::EventSegmentation(es.try_into().unwrap())
            }
            metadata::bookmarks::Query::Funnel(f) => Query::Funnel(f.try_into().unwrap()),
        }
    }
}

impl From<Query> for metadata::bookmarks::Query {
    fn from(value: Query) -> Self {
        match value {
            Query::EventSegmentation(es) => {
                metadata::bookmarks::Query::EventSegmentation(es.try_into().unwrap())
            }
            Query::Funnel(f) => metadata::bookmarks::Query::Funnel(f.try_into().unwrap()),
        }
    }
}

impl From<metadata::bookmarks::Bookmark> for Bookmark {
    fn from(value: metadata::bookmarks::Bookmark) -> Self {
        Bookmark {
            id: value.id,
            created_at: value.created_at,
            created_by: value.created_by,
            project_id: value.project_id,
            query: value.query.into(),
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
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateBookmarkRequest {
    pub query: Query,
}