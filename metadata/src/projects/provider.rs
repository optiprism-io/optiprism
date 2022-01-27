use super::{CreateRequest, ListRequest, Project, UpdateRequest};
use crate::{error::Error, Result, Store};
use bincode::{deserialize, serialize};
use chrono::Utc;
use futures::lock::Mutex;
use std::sync::Arc;

const SEQUENCE_KEY: &[u8] = b"projects/id_seq";
const DATA_PREFIX: &[u8] = b"projects/data/";

fn data_key(organization_id: u64, id: u64) -> Vec<u8> {
    // projects/data/{organization_id}/{id}
    [
        DATA_PREFIX,
        organization_id.to_le_bytes().as_ref(),
        b"/",
        id.to_le_bytes().as_ref(),
    ]
    .concat()
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

    pub async fn create(&self, request: CreateRequest) -> Result<Project> {
        let project = Project {
            id: self.store.next_seq(SEQUENCE_KEY).await?,
            created_at: Utc::now(),
            updated_at: None,
            organization_id: request.organization_id,
            name: request.name,
        };
        self.store
            .put(
                &data_key(project.organization_id, project.id),
                serialize(&project)?,
            )
            .await?;
        Ok(project)
    }

    pub async fn get_by_id(&self, organization_id: u64, id: u64) -> Result<Project> {
        match self.store.get(&data_key(organization_id, id)).await? {
            None => Err(Error::KeyNotFound),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn list(&self, _request: ListRequest) -> Result<Vec<Project>> {
        unimplemented!()
    }

    pub async fn update(&self, request: UpdateRequest) -> Result<Project> {
        let _guard = self.guard.lock().await;
        let mut project = self.get_by_id(request.organization_id, request.id).await?;
        let mut updated = false;
        if let Some(value) = &request.name {
            updated = true;
            project.name = value.clone();
        }
        if updated {
            project.updated_at = Some(Utc::now());
            self.store
                .put(
                    &data_key(project.organization_id, project.id),
                    serialize(&project)?,
                )
                .await?;
        }
        Ok(project)
    }

    pub async fn delete(&self, organization_id: u64, id: u64) -> Result<()> {
        self.get_by_id(organization_id, id).await?;
        self.store.delete(&data_key(organization_id, id)).await?;
        Ok(())
    }
}
