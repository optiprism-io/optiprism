use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::ProjectPermission;
use common::types::OptionalProperty;
use metadata::reports::Reports as MDReports;
use serde::Deserialize;
use serde::Serialize;
use common::event_segmentation::EventSegmentationRequest;

use crate::Context;
use crate::funnel::FunnelRequest;
use crate::ListResponse;
use crate::Result;

pub struct Reports {
    prov: Arc<MDReports>,
}

impl Reports {
    pub fn new(prov: Arc<MDReports>) -> Self {
        Self { prov }
    }
    pub async fn create(
        &self,
        ctx: Context,

        project_id: u64,
        request: CreateReportRequest,
    ) -> Result<Report> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let report = self
            .prov
            .create(project_id, metadata::reports::CreateReportRequest {
                created_by: ctx.account_id,
                tags: request.tags,
                name: request.name,
                description: request.description,
                typ: request.typ.into(),
                query: request.query.into(),
            })?;

        Ok(report.into())
    }

    pub async fn get_by_id(&self, ctx: Context, project_id: u64, id: u64) -> Result<Report> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;

        Ok(self.prov.get_by_id(project_id, id)?.into())
    }

    pub async fn list(&self, ctx: Context, project_id: u64) -> Result<ListResponse<Report>> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ExploreReports,
        )?;
        let resp = self.prov.list(project_id)?;
        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }

    pub async fn update(
        &self,
        ctx: Context,

        project_id: u64,
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        let md_req = metadata::reports::UpdateReportRequest {
            updated_by: ctx.account_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            typ: req.typ.into(),
            query: req.query.into(),
        };

        let report = self.prov.update(project_id, report_id, md_req)?;

        Ok(report.into())
    }

    pub async fn delete(&self, ctx: Context, project_id: u64, id: u64) -> Result<Report> {
        ctx.check_project_permission(
            ctx.organization_id,
            project_id,
            ProjectPermission::ManageReports,
        )?;

        Ok(self.prov.delete(project_id, id)?.into())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    EventSegmentation,
    Funnel,
}

impl From<metadata::reports::Type> for Type {
    fn from(value: metadata::reports::Type) -> Self {
        match value {
            metadata::reports::Type::EventSegmentation => Type::EventSegmentation,
            metadata::reports::Type::Funnel => Type::Funnel,
        }
    }
}

impl From<Type> for metadata::reports::Type {
    fn from(value: Type) -> Self {
        match value {
            Type::EventSegmentation => metadata::reports::Type::EventSegmentation,
            Type::Funnel => metadata::reports::Type::Funnel,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum Query {
    EventSegmentation(EventSegmentationRequest),
    Funnel(FunnelRequest),
}

impl From<metadata::reports::Query> for Query {
    fn from(value: metadata::reports::Query) -> Self {
        match value {
            metadata::reports::Query::EventSegmentation(es) => {
                Query::EventSegmentation(es.try_into().unwrap())
            }
            metadata::reports::Query::Funnel(f) => Query::Funnel(f.try_into().unwrap()),
        }
    }
}

impl From<Query> for metadata::reports::Query {
    fn from(value: Query) -> Self {
        match value {
            Query::EventSegmentation(es) => {
                metadata::reports::Query::EventSegmentation(es.try_into().unwrap())
            }
            Query::Funnel(f) => metadata::reports::Query::Funnel(f.try_into().unwrap()),
        }
    }
}

impl From<metadata::reports::Report> for Report {
    fn from(value: metadata::reports::Report) -> Self {
        Report {
            id: value.id,
            created_at: value.created_at,
            updated_at: value.updated_at,
            created_by: value.created_by,
            updated_by: value.updated_by,
            project_id: value.project_id,
            tags: value.tags,
            name: value.name,
            description: value.description,
            typ: value.typ.into(),
            query: value.query.into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReportRequest {
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReportRequest {
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub tags: OptionalProperty<Option<Vec<String>>>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub name: OptionalProperty<String>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub description: OptionalProperty<Option<String>>,
    #[serde(
        default,
        rename = "type",
        skip_serializing_if = "OptionalProperty::is_none"
    )]
    pub typ: OptionalProperty<Type>,
    #[serde(default, skip_serializing_if = "OptionalProperty::is_none")]
    pub query: OptionalProperty<Query>,
}
