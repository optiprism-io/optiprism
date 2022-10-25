use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::Utc;
use common::types::OptionalProperty;
use tokio::sync::RwLock;

use crate::error;
use crate::error::MetadataError;
use crate::error::StoreError;
use crate::error::TeamError;
use crate::metadata::ListResponse;
use crate::store::index::hash_map::HashMap;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_ns;
use crate::store::Store;
use crate::teams::types::CreateTeamRequest;
use crate::teams::types::Team;
use crate::teams::types::UpdateTeamRequest;
use crate::Result;

const NAMESPACE: &[u8] = b"teams";
const IDX_NAME: &[u8] = b"name";

fn index_keys(organization_id: u64, name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(organization_id, name)].to_vec()
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

    pub async fn create(&self, organization_id: u64, req: CreateTeamRequest) -> Result<Team> {
        let _guard = self.guard.write().await;

        let idx_keys = index_keys(organization_id, &req.name);

        match self.idx.check_insert_constraints(idx_keys.as_ref()).await {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(TeamError::TeamAlreadyExist(error::Team::new_with_name(
                    organization_id,
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
            .next_seq(make_id_seq_key(
                org_ns(organization_id, NAMESPACE).as_slice(),
            ))
            .await?;

        let team = Team {
            id,
            created_at,
            updated_at: None,
            created_by: req.created_by,
            updated_by: None,
            organization_id,
            name: req.name,
        };
        let data = serialize(&team)?;
        self.store
            .put(
                make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team.id),
                &data,
            )
            .await?;

        self.idx.insert(idx_keys.as_ref(), &data).await?;

        Ok(team)
    }

    pub async fn get_by_id(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id);

        match self.store.get(key).await? {
            None => Err(TeamError::TeamNotFound(error::Team::new_with_id(
                organization_id,
                team_id,
            ))
            .into()),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub async fn list(&self, organization_id: u64) -> Result<ListResponse<Team>> {
        list(
            self.store.clone(),
            org_ns(organization_id, NAMESPACE).as_slice(),
        )
        .await
    }

    pub async fn update(
        &self,
        organization_id: u64,
        team_id: u64,
        req: UpdateTeamRequest,
    ) -> Result<Team> {
        let _guard = self.guard.write().await;

        let prev_team = self.get_by_id(organization_id, team_id).await?;
        let mut team = prev_team.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_team.name.as_str()));
            team.name = name.to_owned();
        }

        match self
            .idx
            .check_update_constraints(idx_keys.as_ref(), idx_prev_keys.as_ref())
            .await
        {
            Err(MetadataError::Store(StoreError::KeyAlreadyExists(_))) => {
                return Err(TeamError::TeamAlreadyExist(error::Team::new_with_id(
                    organization_id,
                    team_id,
                ))
                .into());
            }
            Err(other) => return Err(other),
            Ok(_) => {}
        }

        team.updated_at = Some(Utc::now());
        team.updated_by = Some(req.updated_by);

        let data = serialize(&team)?;
        self.store
            .put(
                make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id),
                &data,
            )
            .await?;

        self.idx
            .update(idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)
            .await?;

        Ok(team)
    }

    pub async fn delete(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let _guard = self.guard.write().await;
        let team = self.get_by_id(organization_id, team_id).await?;
        self.store
            .delete(make_data_value_key(
                org_ns(organization_id, NAMESPACE).as_slice(),
                team_id,
            ))
            .await?;

        self.idx
            .delete(index_keys(organization_id, &team.name).as_ref())
            .await?;

        Ok(team)
    }
}
