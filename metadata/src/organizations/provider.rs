use super::{CreateRequest, ListRequest, Organization, UpdateRequest};
use crate::store::Store;
use crate::{error, error::Error, Result};
use bincode::{deserialize, serialize};
use chrono::Utc;
use futures::lock::Mutex;
use std::sync::Arc;
use crate::error::OrganizationError;

const SEQUENCE_KEY: &str = "organizations/id_seq";
const DATA_PREFIX: &str = "organizations/data/";

fn data_key(id: u64) -> Vec<u8> {
    format!("{DATA_PREFIX}{id}").into()
}

pub struct Provider {
    store: Arc<Store>,
    guard: Mutex<()>,
}

impl Provider {
    pub fn new(store: Arc<Store>) -> Self {
        Provider {
            store,
            guard: Mutex::new(()),
        }
    }

    pub async fn create(&self, request: CreateRequest) -> Result<Organization> {
        let organization = Organization {
            id: self.store.next_seq(SEQUENCE_KEY).await?,
            created_at: Utc::now(),
            updated_at: None,
            name: request.name,
        };
        self.store
            .put(&data_key(organization.id), serialize(&organization)?)
            .await?;
        Ok(organization)
    }

    pub async fn get_by_id(&self, id: u64) -> Result<Organization> {
        match self.store.get(&data_key(id)).await? {
            None => Err(OrganizationError::OrganizationNotFound(error::Organization::new(id)).into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn list(&self, _: ListRequest) -> Result<Vec<Organization>> {
        let mut list = self
            .store
            .list_prefix(DATA_PREFIX)
            .await?
            .iter()
            .map(|v| deserialize(v.1.as_ref()))
            .collect::<bincode::Result<Vec<Organization>>>()?;
        list.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(list)
    }

    pub async fn update(&self, request: UpdateRequest) -> Result<Organization> {
        let _guard = self.guard.lock().await;
        let mut organization = self.get_by_id(request.id).await?;
        let mut updated = false;
        if let Some(value) = &request.name {
            updated = true;
            organization.name = value.clone();
        }
        if updated {
            organization.updated_at = Some(Utc::now());
            self.store
                .put(&data_key(organization.id), serialize(&organization)?)
                .await?;
        }
        Ok(organization)
    }

    pub async fn delete(&self, id: u64) -> Result<()> {
        self.get_by_id(id).await?;
        self.store.delete(&data_key(id)).await?;
        Ok(())
    }
}
