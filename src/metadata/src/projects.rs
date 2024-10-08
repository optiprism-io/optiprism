use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
use prost::Message;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;

use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::project;
use crate::Result;

const NAMESPACE: &[u8] = b"projects";
const IDX_NAME: &[u8] = b"name";

fn index_keys(name: &str, token: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(name), index_token_key(token)].to_vec()
}

fn index_name_key(name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_NAME, name).to_vec())
}

fn index_token_key(token: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_NAME, token).to_vec())
}

pub struct Projects {
    db: Arc<TransactionDB>,
}

impl Projects {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Projects { db }
    }

    fn get_by_id_(&self, tx: &Transaction<TransactionDB>, project_id: u64) -> Result<Project> {
        let key = make_data_value_key(NAMESPACE, project_id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(format!(
                "project {project_id} not found"
            ))),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, req: CreateProjectRequest) -> Result<Project> {
        let tx = self.db.transaction();
        let idx_keys = index_keys(&req.name, req.token.as_str());
        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let project = Project {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            organization_id: req.organization_id,
            name: req.name,
            description: req.description,
            tags: req.tags,
            token: req.token,
            session_duration_seconds: req.session_duration_seconds,
            events_count: 0,
        };
        let data = serialize(&project)?;
        tx.put(make_data_value_key(NAMESPACE, project.id), data)?;

        insert_index(&tx, idx_keys.as_ref(), project.id)?;
        tx.commit()?;
        Ok(project)
    }

    pub fn get_by_id(&self, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, project_id)
    }

    pub fn get_by_token(&self, token: &str) -> Result<Project> {
        let tx = self.db.transaction();
        let id = get_index(
            &tx,
            index_token_key(token).unwrap(),
            "project can't be found by token",
        )?;
        self.get_by_id_(&tx, id)
    }

    pub fn list(&self) -> Result<ListResponse<Project>> {
        let tx = self.db.transaction();
        let prefix = [NAMESPACE, b"/data"].concat();

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix)
                .unwrap()
                .is_prefix_of(from_utf8(&key).unwrap())
            {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update(&self, project_id: u64, req: UpdateProjectRequest) -> Result<Project> {
        let tx = self.db.transaction();

        let prev_project = self.get_by_id_(&tx, project_id)?;
        let mut project = prev_project.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(name.as_str()));
            idx_prev_keys.push(index_name_key(prev_project.name.as_str()));
            project.name.clone_from(name);
        }
        if let OptionalProperty::Some(token) = &req.token {
            idx_keys.push(index_token_key(token.as_str()));
            idx_prev_keys.push(index_token_key(prev_project.token.as_str()));
            project.token.clone_from(token)
        }
        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        project.updated_at = Some(Utc::now());
        project.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(tags) = req.tags {
            project.tags = tags;
        }
        if let OptionalProperty::Some(description) = req.description {
            project.description = description;
        }

        if let OptionalProperty::Some(session_duration) = req.session_duration_seconds {
            project.session_duration_seconds = session_duration;
        }

        let data = serialize(&project)?;
        tx.put(make_data_value_key(NAMESPACE, project_id), data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), project_id)?;
        tx.commit()?;
        Ok(project)
    }

    pub fn increment_events_counter(&self, project_id: u64) -> Result<()> {
        let tx = self.db.transaction();

        let mut project = self.get_by_id_(&tx, project_id)?;
        project.events_count += 1;
        let data = serialize(&project)?;
        tx.put(make_data_value_key(NAMESPACE, project_id), data)?;
        tx.commit()?;

        Ok(())
    }
    pub fn delete(&self, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();

        let project = self.get_by_id_(&tx, project_id)?;
        tx.delete(make_data_value_key(NAMESPACE, project_id))?;

        delete_index(&tx, index_keys(&project.name, &project.token).as_ref())?;
        tx.commit()?;
        Ok(project)
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub token: String,
    pub session_duration_seconds: u64,
    pub events_count: usize,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProjectRequest {
    pub created_by: u64,
    pub organization_id: u64,
    pub name: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub token: String,
    pub session_duration_seconds: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProjectRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub token: OptionalProperty<String>,
    pub session_duration_seconds: OptionalProperty<u64>,
}

// serialize project to protobuf
fn serialize(project: &Project) -> Result<Vec<u8>> {
    let tags = if let Some(tags) = &project.tags {
        tags.to_vec()
    } else {
        vec![]
    };

    let v = project::Project {
        id: project.id,
        created_at: project.created_at.timestamp(),
        created_by: project.created_by,
        updated_at: project.updated_at.map(|t| t.timestamp()),
        updated_by: project.updated_by,
        organization_id: project.organization_id,
        name: project.name.clone(),
        description: project.description.clone(),
        tags,
        token: project.token.clone(),
        session_duration_seconds: project.session_duration_seconds,
        events_count: project.events_count as u64,
    };
    Ok(v.encode_to_vec())
}

// deserialize project from protobuf
fn deserialize(data: &[u8]) -> Result<Project> {
    let from = project::Project::decode(data)?;

    Ok(Project {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from
            .updated_at
            .map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        organization_id: from.organization_id,
        name: from.name,
        description: from.description,
        tags: if from.tags.is_empty() {
            None
        } else {
            Some(from.tags)
        },
        token: from.token,
        session_duration_seconds: from.session_duration_seconds,
        events_count: from.events_count as usize,
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use crate::projects::deserialize;
    use crate::projects::serialize;
    use crate::projects::Project;

    #[test]
    fn test_roundtrip() {
        let project = Project {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            created_by: 1,
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            updated_by: Some(2),
            organization_id: 1,
            name: "test".to_string(),
            description: Some("test".to_string()),
            tags: Some(vec!["test".to_string()]),
            token: "test".to_string(),
            session_duration_seconds: 1,
            events_count: 1,
        };

        let data = serialize(&project).unwrap();
        let project2 = deserialize(&data).unwrap();
        assert_eq!(project, project2);
    }
}
