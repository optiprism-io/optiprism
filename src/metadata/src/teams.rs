use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::metadata::ListResponse;
use crate::store::path_helpers::list;
use crate::store::path_helpers::make_data_value_key;
use crate::store::path_helpers::make_id_seq_key;
use crate::store::path_helpers::make_index_key;
use crate::store::path_helpers::org_ns;
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

pub struct Teams {
    db: Arc<TransactionDB>,
}

impl Teams {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Teams { db }
    }

    fn _get_by_id(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        team_id: u64,
    ) -> Result<Team> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound("team not found".to_string())),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, organization_id: u64, req: CreateTeamRequest) -> Result<Team> {
        let tx = self.db.transaction();

        let idx_keys = index_keys(organization_id, &req.name);

        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(
            &tx,
            make_id_seq_key(org_ns(organization_id, NAMESPACE).as_slice()),
        )?;

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
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team.id),
            &data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(team)
    }

    pub fn get_by_id(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let tx = self.db.transaction();

        self._get_by_id(&tx, organization_id, team_id)
    }

    pub fn list(&self, organization_id: u64) -> Result<ListResponse<Team>> {
        let tx = self.db.transaction();

        list(&tx, org_ns(organization_id, NAMESPACE).as_slice())
    }

    pub fn update(
        &self,
        organization_id: u64,
        team_id: u64,
        req: UpdateTeamRequest,
    ) -> Result<Team> {
        let tx = self.db.transaction();

        let prev_team = self._get_by_id(&tx, organization_id, team_id)?;
        let mut team = prev_team.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_team.name.as_str()));
            team.name = name.to_owned();
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        team.updated_at = Some(Utc::now());
        team.updated_by = Some(req.updated_by);

        let data = serialize(&team)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id),
            &data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), &data)?;
        tx.commit()?;
        Ok(team)
    }

    pub fn delete(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let tx = self.db.transaction();

        let team = self._get_by_id(&tx, organization_id, team_id)?;
        tx.delete(make_data_value_key(
            org_ns(organization_id, NAMESPACE).as_slice(),
            team_id,
        ))?;

        delete_index(&tx, index_keys(organization_id, &team.name).as_ref())?;
        tx.commit()?;
        Ok(team)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Team {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateTeamRequest {
    pub created_by: u64,
    pub organization_id: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateTeamRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}
