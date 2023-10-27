use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::thread_rng;
use rocksdb::{Transaction, TransactionDB};

use crate::error;
use crate::error::MetadataError;
use crate::index::{check_insert_constraints, check_update_constraints, delete_index, get_index, insert_index, next_seq, update_index};
use crate::metadata::ListResponse;
use crate::projects::CreateProjectRequest;
use crate::projects::Project;
use crate::projects::Provider;
use crate::projects::UpdateProjectRequest;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_ns;
use crate::store::Store;
use crate::Result;

const NAMESPACE: &[u8] = b"projects";
const IDX_NAME: &[u8] = b"name";
const IDX_TOKEN: &[u8] = b"token";

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

pub struct ProviderImpl {
    db: Arc<TransactionDB>,
}

impl ProviderImpl {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ProviderImpl { db }
    }

    fn _get_by_id(&self, tx: &Transaction<TransactionDB>, organization_id: u64, project_id: u64) -> Result<Project> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(format!("project {project_id}"))),
            Some(value) => Ok(deserialize(&value)?),
        }
    }
}

impl Provider for ProviderImpl {
    fn create(&self, organization_id: u64, req: CreateProjectRequest) -> Result<Project> {
        let tx = self.db.transaction();
        let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

        let idx_keys = index_keys(organization_id, &req.name, token.as_str());

        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(
            org_ns(organization_id, NAMESPACE).as_slice(),
        ))?;

        let project = Project {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            organization_id,
            name: req.name,
            token,
        };
        let data = serialize(&project)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project.id),
            &data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;

        Ok(project)
    }

    fn get_by_id(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();
        self._get_by_id(&tx, organization_id, project_id)
    }

    fn get_by_token(&self, token: &str) -> Result<Project> {
        let tx = self.db.transaction();
        let data = get_index(&tx,index_token_key(token).unwrap())?;
        Ok(deserialize(&data)?)
    }

    fn list(&self, organization_id: u64) -> Result<ListResponse<Project>> {
        let tx = self.db.transaction();

        list(
            &tx,
            org_ns(organization_id, NAMESPACE).as_slice(),
        )
    }

    fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        let tx = self.db.transaction();


        let prev_project = self._get_by_id(&tx, organization_id, project_id)?;
        let mut project = prev_project.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_project.name.as_str()));
            project.name = name.to_owned();
        }

        check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())?

        project.updated_at = Some(Utc::now());
        project.updated_by = Some(req.updated_by);

        let data = serialize(&project)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;

        Ok(project)
    }

    fn delete(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let tx = self.db.transaction();

        let project = self._get_by_id(&tx,organization_id,project_id)?;
        tx.delete(make_data_value_key(NAMESPACE, project_id))?;

        delete_index(&tx, index_keys(organization_id,&project.name,&project.token).as_ref())?;

        Ok(project)
    }
}
