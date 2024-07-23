use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use crate::bookmarks::Bookmark;
use crate::error::MetadataError;
use crate::index::next_seq;
use crate::{bookmark, dashboard, list_data};
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::metadata::ListResponse;
use crate::project_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"dashboards";

pub struct Dashboards {
    db: Arc<TransactionDB>,
}

impl Dashboards {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Dashboards { db }
    }

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        project_id: u64,
        id: u64,
    ) -> Result<Dashboard> {
        let key = make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("dashboard {} not found", id).to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, project_id: u64, req: CreateDashboardRequest) -> Result<Dashboard> {
        let tx = self.db.transaction();
        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(project_ns(project_id, NAMESPACE).as_slice()),
        )?;

        let dashboard = Dashboard {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            project_id,
            tags: req.tags,
            name: req.name,
            description: req.description,
            panels: req.panels,
        };
        let data = serialize(&dashboard)?;
        tx.put(
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), dashboard.id),
            data,
        )?;

        tx.commit()?;
        Ok(dashboard)
    }

    pub fn get_by_id(&self, project_id: u64, id: u64) -> Result<Dashboard> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, project_id, id)
    }

    pub fn list(&self, project_id: u64) -> Result<ListResponse<Dashboard>> {
        let tx = self.db.transaction();
        list_data(&tx, project_ns(project_id, NAMESPACE).as_slice())
    }

    pub fn update(
        &self,
        project_id: u64,
        dashboard_id: u64,
        req: UpdateDashboardRequest,
    ) -> Result<Dashboard> {
        let tx = self.db.transaction();

        let prev_dashboard = self.get_by_id_(&tx, project_id, dashboard_id)?;
        let mut dashboard = prev_dashboard.clone();

        dashboard.updated_at = Some(Utc::now());
        dashboard.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(name) = req.name {
            dashboard.name = name;
        }
        if let OptionalProperty::Some(description) = req.description {
            dashboard.description = description;
        }
        if let OptionalProperty::Some(tags) = req.tags {
            dashboard.tags = tags;
        }
        if let OptionalProperty::Some(panels) = req.panels {
            dashboard.panels = panels;
        }

        let data = serialize(&dashboard)?;
        tx.put(
            make_data_value_key(project_ns(project_id, NAMESPACE).as_slice(), dashboard.id),
            data,
        )?;
        tx.commit()?;
        Ok(dashboard)
    }

    pub fn delete(&self, project_id: u64, id: u64) -> Result<Dashboard> {
        let tx = self.db.transaction();
        let dashboard = self.get_by_id_(&tx, project_id, id)?;
        tx.delete(make_data_value_key(
            project_ns(project_id, NAMESPACE).as_slice(),
            id,
        ))?;
        tx.commit()?;
        Ok(dashboard)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Type {
    Report,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Panel {
    #[serde(rename = "type")]
    pub typ: Type,
    pub report_id: u64,
    pub x: usize,
    pub y: usize,
    pub w: usize,
    pub h: usize,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Dashboard {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub panels: Vec<Panel>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CreateDashboardRequest {
    pub created_by: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
    pub panels: Vec<Panel>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UpdateDashboardRequest {
    pub updated_by: u64,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub panels: OptionalProperty<Vec<Panel>>,
}

fn serialize(v: &Dashboard) -> Result<Vec<u8>> {
    let tags = if let Some(tags) = &v.tags {
        tags.to_vec()
    } else { vec![] };

    let panels = v.panels.iter().map(|p| dashboard::Panel {
        r#type: match p.typ {
            Type::Report => 1,
        },
        report_id: p.report_id,
        x: p.x as u32,
        y: p.y as u32,
        w: p.w as u32,
        h: p.h as u32,
    }).collect::<Vec<_>>();

    let d = dashboard::Dashboard {
        id: v.id,
        created_at: v.created_at.timestamp(),
        created_by: v.created_by,
        updated_at: v.updated_at.map(|t| t.timestamp()),
        updated_by: v.updated_by,
        project_id: v.project_id,
        tags,
        name: v.name.clone(),
        description: v.description.clone(),
        panels,
    };
    Ok(d.encode_to_vec())
}

fn deserialize(data: &Vec<u8>) -> Result<Dashboard> {
    let from = dashboard::Dashboard::decode(data.as_ref())?;

    Ok(Dashboard {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        project_id: from.project_id,
        tags: if from.tags.is_empty() { None } else { Some(from.tags) },
        name: from.name,
        description: from.description,
        panels: from.panels.iter().map(|p| Panel {
            typ: match p.r#type {
                1 => Type::Report,
                _ => unreachable!(),
            },
            report_id: p.report_id,
            x: p.x as usize,
            y: p.y as usize,
            w: p.w as usize,
            h: p.h as usize,
        }).collect::<Vec<_>>()
    })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use crate::dashboards::{Dashboard, deserialize, serialize};

    #[test]
    fn test_roundtrip() {
        let d = Dashboard {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            updated_at: Some(DateTime::from_timestamp(2, 0)).unwrap(),
            created_by: 1,
            updated_by: Some(2),
            project_id: 3,
            tags: Some(vec!["tag1".to_string(), "tag2".to_string()]),
            name: "test".to_string(),
            description: Some("test description".to_string()),
            panels: vec![
                crate::dashboards::Panel {
                    typ: crate::dashboards::Type::Report,
                    report_id: 1,
                    x: 1,
                    y: 2,
                    w: 3,
                    h: 4,
                },
            ],
        };

        let data = serialize(&d).unwrap();
        let d2 = deserialize(&data).unwrap();
        assert_eq!(d, d2);
    }
}
