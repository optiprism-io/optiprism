use super::{CreateOrganizationRequest, Organization};
use crate::error::{MetadataError, OrganizationError, StoreError};
use crate::store::Store;
use crate::{error, Result};
use bincode::{deserialize, serialize};
use chrono::Utc;
use futures::lock::Mutex;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::metadata::ListResponse;
use crate::organizations::types::UpdateOrganizationRequest;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::{list, make_data_value_key, make_id_seq_key, make_index_key};

const NAMESPACE: &[u8] = b"organizations";
const IDX_NAME: &[u8] = b"name";

fn index_keys(name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(name)].to_vec()
}

fn index_name_key(name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_NAME, name).to_vec())
}

pub struct Provider {
    store: Arc<Store>,
    idx: HashMap,
    guard: RwLock<()>,
}

impl Provider {
    pub fn new(kv: Arc<Store>) -> Self {
        Provider {
            store: kv.clone(),
            idx: HashMap::new(kv),
            guard: RwLock::new(()),
        }
    }

    pub async fn create(
        &self,
        req: CreateOrganizationRequest,
    ) -> Result<Organization> {
        let _guard = self.guard.write().await;

        let idx_keys = index_keys(
            &req.name,
        );
        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(OrganizationError::OrganizationAlreadyExist(error::Organization::new_with_name(
                    req.name,
                ))
                    .into());
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        let created_at = Utc::now();
        let id = self
            .store
            .next_seq(make_id_seq_key(NAMESPACE))
            .await?;

        let org = Organization {
            id,
            created_at,
            created_by: req.created_by,
            updated_at: None,
            updated_by: None,
            name: req.name,
        };

        let data = serialize(&org)?;
        self.store
            .put(
                make_data_value_key(NAMESPACE, id),
                &data,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;

        Ok(org)
    }

    pub async fn get_by_id(&self, id: u64) -> Result<Organization> {
        let key = make_data_value_key(NAMESPACE, id);

        match self.store.get(key).await? {
            None => Err(OrganizationError::OrganizationNotFound(error::Organization::new_with_id(
                id,
            ))
                .into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn list(&self) -> Result<ListResponse<Organization>> {
        list(self.store.clone(), NAMESPACE).await
    }

    pub async fn update(
        &self,
        org_id: u64,
        req: UpdateOrganizationRequest,
    ) -> Result<Organization> {
        let _guard = self.guard.write().await;

        let prev_org = self
            .get_by_id(org_id)
            .await?;

        let mut org = prev_org.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let Some(name) = &req.name {
            idx_keys.push(index_name_key(name.as_str()));
            idx_prev_keys.push(index_name_key(
                prev_org.name.as_str(),
            ));
            org.name = name.to_owned();
        }

        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(OrganizationError::OrganizationAlreadyExist(error::Organization::new_with_id(
                    org_id,
                ))
                    .into());
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        org.updated_at = Some(Utc::now());
        org.updated_by = Some(req.updated_by);

        let data = serialize(&org)?;
        self.store
            .put(
                make_data_value_key(NAMESPACE, org.id),
                &data,
            )
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;

        Ok(org)
    }

    pub async fn delete(&self, id: u64) -> Result<Organization> {
        let _guard = self.guard.write().await;
        let org = self.get_by_id(id).await?;
        self.store
            .delete(make_data_value_key(
                NAMESPACE,
                id,
            ))
            .await?;

        self.idx
            .delete(
                index_keys(
                    &org.name,
                )
                    .as_ref(),
            )
            .await?;

        Ok(org)
    }
}
