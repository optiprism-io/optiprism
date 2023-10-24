use std::sync::Arc;

use async_trait::async_trait;
use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use rand::thread_rng;
use tokio::sync::RwLock;

use crate::error;
use crate::error::MetadataError;
use crate::error::ProjectError;
use crate::error::StoreError;
use crate::metadata::ListResponse;
use crate::projects::CreateProjectRequest;
use crate::projects::Project;
use crate::projects::Provider;
use crate::projects::UpdateProjectRequest;
use crate::store::index::hash_map::StoreHashMap;
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
    store: Arc<Store>,
    idx: StoreHashMap,
    guard: RwLock<()>,
}

impl ProviderImpl {
    pub fn new(kv: Arc<Store>) -> Self {
        ProviderImpl {
            store: kv.clone(),
            idx: StoreHashMap::new(kv),
            guard: RwLock::new(()),
        }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn create(&self, organization_id: u64, req: CreateProjectRequest) -> Result<Project> {
        let _guard = self.guard.write().await;
        let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

        let idx_keys = index_keys(organization_id, &req.name, token.as_str());

        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(MetadataError::AlreadyExists(format!(
                    "project with name {:?}",
                    req.name
                )));
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(
                org_ns(organization_id, NAMESPACE).as_slice(),
            ))
            .await?;

        let project = Project {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            organization_id,
            name: req.name,
            token,
            sdk: req.sdk,
        };
        let data = serialize(&project)?;
        self.store
            .put(
                make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project.id),
                &data,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;

        Ok(project)
    }

    async fn get_by_id(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id);

        match self.store.get(key).await? {
            None => Err(MetadataError::NotFound(format!("project {project_id}"))),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    async fn get_by_token(&self, token: &str) -> Result<Project> {
        match self.idx.get(index_token_key(token).unwrap()).await {
            Err(MetadataError::Store(StoreError::KeyNotFound(_))) => Err(MetadataError::NotFound(
                format!("project with token {:?}", token),
            )),
            Err(other) => Err(other),
            Ok(data) => Ok(deserialize(&data)?),
        }
    }

    async fn list(&self, organization_id: u64) -> Result<ListResponse<Project>> {
        list(
            self.store.clone(),
            org_ns(organization_id, NAMESPACE).as_slice(),
        )
        .await
    }

    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project> {
        let _guard = self.guard.write().await;

        let prev_project = self.get_by_id(organization_id, project_id).await?;
        let mut project = prev_project.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_project.name.as_str()));
            project.name = name.to_owned();
        }

        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(MetadataError::AlreadyExists(format!(
                    "project {:?}",
                    req.name
                )));
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        project.updated_at = Some(Utc::now());
        project.updated_by = Some(req.updated_by);

        let data = serialize(&project)?;
        self.store
            .put(
                make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), project_id),
                &data,
            )
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;

        Ok(project)
    }

    async fn delete(&self, organization_id: u64, project_id: u64) -> Result<Project> {
        let _guard = self.guard.write().await;
        let project = self.get_by_id(organization_id, project_id).await?;
        self.store
            .delete(make_data_value_key(
                org_ns(organization_id, NAMESPACE).as_slice(),
                project_id,
            ))
            .await?;

        self.idx
            .delete(index_keys(organization_id, &project.name, project.token.as_str()).as_ref())
            .await?;

        Ok(project)
    }
}
