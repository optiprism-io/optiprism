use std::sync::Arc;
use std::time;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::types::OptionalProperty;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::thread_rng;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::DurationSeconds;

use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::list;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::org_ns;
use crate::Result;

const NAMESPACE: &[u8] = b"projects";
const IDX_NAME: &[u8] = b"name";

fn index_keys(organization_id: u64, name: &str, token: &str) -> Vec<Option<Vec<u8>>> {
    [
        index_name_key(organization_id, name),
        index_token_key(token),
    ]
    .to_vec()
}

fn index_name_key(organization_id: u64, name: &str) -> Option<Vec<u8>> {
    Some(
        make_index_key(
            org_ns(organization_id, NAMESPACE).as_slice(),
            IDX_NAME,
            name,
        )
        .to_vec(),
    )
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

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        project_id: u64,
    ) -> Result<Project> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(format!("project {project_id}"))),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, organization_id: u64, req: CreateProjectRequest) -> Result<Project> {
        let tx = self.db.transaction();
        let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

        let idx_keys = index_keys(organization_id, &req.name, token.as_str());

        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_ns(organization_id, NAMESPACE).as_slice()),
        )?;

        let project = Project {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            organization_id,
            name: req.name,
            description: req.description,
            tags: req.tags,
            token,
            session_duration: req.session_duration,
        };
        let data = serialize(&project)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project.id),
            &data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(project)
    }

    pub fn get_by_id(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, organization_id, project_id)
    }

    pub fn get_by_token(&self, token: &str) -> Result<Project> {
        let tx = self.db.transaction();
        let data = get_index(&tx, index_token_key(token).unwrap())?;
        Ok(deserialize::<Project>(&data)?)
    }

    pub fn list(&self, organization_id: u64) -> Result<ListResponse<Project>> {
        let tx = self.db.transaction();

        list(&tx, org_ns(organization_id, NAMESPACE).as_slice())
    }

    pub fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        let tx = self.db.transaction();

        let prev_project = self.get_by_id_(&tx, organization_id, project_id)?;
        let mut project = prev_project.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_project.name.as_str()));
            project.name = name.to_owned();
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

        if let OptionalProperty::Some(session_duration) = req.session_duration {
            project.session_duration = session_duration;
        }

        let data = serialize(&project)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(project)
    }

    pub fn delete(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();

        let project = self.get_by_id_(&tx, organization_id, project_id)?;
        tx.delete(make_data_value_key(NAMESPACE, project_id))?;

        delete_index(
            &tx,
            index_keys(organization_id, &project.name, &project.token).as_ref(),
        )?;
        tx.commit()?;
        Ok(project)
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
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
    pub session_duration: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProjectRequest {
    pub created_by: u64,
    pub name: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub session_duration: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProjectRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
    pub description: OptionalProperty<Option<String>>,
    pub tags: OptionalProperty<Option<Vec<String>>>,
    pub session_duration: OptionalProperty<u64>,
}
