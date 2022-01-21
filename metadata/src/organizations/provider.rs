use super::{CreateRequest, ListRequest, Organization, UpdateRequest};
use crate::{error::Error, Result, Store};
use bincode::{deserialize, serialize};
use chrono::Utc;
use futures::lock::Mutex;
use std::sync::Arc;

const SEQUENCE_KEY: &[u8] = b"organizations/id_seq";
const DATA_PREFIX: &[u8] = b"organizations/data/";

fn data_key(id: u64) -> Vec<u8> {
    // organizations/data/{id}
    [DATA_PREFIX, id.to_le_bytes().as_ref()].concat()
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
            None => Err(Error::OrganizationDoesNotExist),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn list(&self, _request: ListRequest) -> Result<Vec<Organization>> {
        unimplemented!()
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
