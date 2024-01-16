use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::EventSegmentation;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::index::next_seq;
use crate::list_data;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::metadata::ListResponse;
use crate::org_proj_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"reports";

pub struct Reports {
    db: Arc<TransactionDB>,
}
impl Reports {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Reports { db }
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
        id: u64,
    ) -> Result<Report> {
        let key = make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        );

        match tx.get(key)? {
            None => Err(MetadataError::NotFound("report not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(
        &self,
        organization_id: u64,
        project_id: u64,
        req: CreateReportRequest,
    ) -> Result<Report> {
        let tx = self.db.transaction();

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_proj_ns(organization_id, project_id, NAMESPACE).as_slice()),
        )?;

        let report = Report {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            typ: req.typ,
            query: req.query,
        };
        let data = serialize(&report)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    pub fn get_by_id(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, organization_id, project_id, id)
    }

    pub fn list(&self, organization_id: u64, project_id: u64) -> Result<ListResponse<Report>> {
        let tx = self.db.transaction();
        list_data(
            &tx,
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
        )
    }

    pub fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        let tx = self.db.transaction();
        let prev_report = self.get_by_id_(&tx, organization_id, project_id, report_id)?;
        let mut report = prev_report.clone();

        report.updated_at = Some(Utc::now());
        report.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(name) = req.name {
            report.name = name;
        }
        if let OptionalProperty::Some(description) = req.description {
            report.description = description;
        }
        if let OptionalProperty::Some(tags) = req.tags {
            report.tags = tags;
        }
        if let OptionalProperty::Some(query) = req.query {
            report.query = query;
        }

        let data = serialize(&report)?;
        tx.put(
            make_data_value_key(
                org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
                report.id,
            ),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    pub fn delete(&self, organization_id: u64, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();
        let report = self.get_by_id_(&tx, organization_id, project_id, id)?;
        tx.delete(make_data_value_key(
            org_proj_ns(organization_id, project_id, NAMESPACE).as_slice(),
            id,
        ))?;
        tx.commit()?;
        Ok(report)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    EventSegmentation,
    Funnel,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Query {
    EventSegmentation(EventSegmentation),
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
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub typ: Type,
    pub query: Query,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UpdateReportRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub typ: OptionalProperty<Type>,
    pub query: OptionalProperty<Query>,
}
