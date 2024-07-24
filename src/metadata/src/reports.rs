use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use common::event_segmentation::EventSegmentationRequest;
use common::funnel::Funnel;

use crate::error::MetadataError;
use crate::index::next_seq;
use crate::{list_data, make_data_key, report, reports};
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::metadata::{ListResponse, ResponseMetadata};
use crate::project_ns;
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
        project_id: u64,
        id: u64,
    ) -> Result<Report> {
        let key = make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("report {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, project_id: u64, req: CreateReportRequest) -> Result<Report> {
        let tx = self.db.transaction();

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(project_ns(project_id, NAMESPACE).as_slice()),
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
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), report.id),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    pub fn get_by_id(&self, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, project_id, id)
    }

    pub fn list(&self, project_id: u64) -> Result<ListResponse<Report>> {
        let tx = self.db.transaction();

        let prefix = make_data_key(project_ns(project_id, NAMESPACE).as_slice());

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !prefix.as_slice().cmp(&key[..prefix.len()]).is_eq() {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update(
        &self,
        project_id: u64,
        report_id: u64,
        req: UpdateReportRequest,
    ) -> Result<Report> {
        let tx = self.db.transaction();
        let prev_report = self.get_by_id_(&tx, project_id, report_id)?;
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
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), report.id),
            data,
        )?;
        tx.commit()?;
        Ok(report)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<Report> {
        let tx = self.db.transaction();
        let report = self.get_by_id_(&tx, project_id, id)?;
        tx.delete(make_data_value_key(
            project_ns(project_id, NAMESPACE).as_slice(),
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
    EventSegmentation(EventSegmentationRequest),
    Funnel(Funnel),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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
pub struct UpdateReportRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub typ: OptionalProperty<Type>,
    pub query: OptionalProperty<Query>,
}

// serialize report into protobuf
fn serialize_report(r: &Report) -> Result<Vec<u8>> {
    let v = report::Report {
        id: r.id,
        created_at: r.created_at.timestamp(),
        updated_at: r.updated_at.map(|t| t.timestamp()),
        created_by: r.created_by,
        updated_by: r.updated_by,
        project_id: r.project_id,
        tags: r.tags.clone().unwrap_or_default(),
        name: r.name.clone(),
        description: r.description.clone(),
        query: serialize(&r.query)?,
        r#type: match r.typ {
            Type::EventSegmentation => report::Type::EventSegmentation,
            Type::Funnel => report::Type::Funnel,
        } as i32,
    };

    Ok(v.encode_to_vec())
}

// deserialize report from protobuf
fn deserialize_report(data: &[u8]) -> Result<Report> {
    let from = report::Report::decode(data.as_ref())?;

    Ok(Report {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        created_by: from.created_by,
        updated_by: from.updated_by,
        project_id: from.project_id,
        tags: if from.tags.is_empty() {
            None
        } else {
            Some(from.tags)
        },
        name: from.name,
        description: from.description,
        typ: match from.r#type {
            1 => Type::EventSegmentation,
            2 => Type::Funnel,
            _ => unreachable!(),
        },
        query: deserialize(&from.query)?,
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use common::event_segmentation::{Analysis, ChartType, EventSegmentationRequest};
    use common::query::{QueryTime, TimeIntervalUnit};
    use crate::reports::{deserialize_report, Query, Report, serialize_report, Type};

    #[test]
    fn test_roundtrip(){
        let report = Report {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            created_by: 1,
            updated_by: Some(2),
            project_id: 1,
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            name: "name".to_string(),
            description: Some("description".to_string()),
            typ: Type::EventSegmentation,
            query: Query::EventSegmentation(EventSegmentationRequest {
                time: QueryTime::Last { last: 1, unit: TimeIntervalUnit::Day },
                group_id: 0,
                interval_unit: TimeIntervalUnit::Hour,
                chart_type: ChartType::Line,
                analysis: Analysis::Linear,
                compare: None,
                events: vec![],
                filters: None,
                breakdowns: None,
                segments: None,
            }),
        };

        let data = serialize_report(&report).unwrap();
        let from = deserialize_report(&data).unwrap();

        assert_eq!(report, from);
    }
}