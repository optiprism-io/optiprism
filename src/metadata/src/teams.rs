use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
use prost::Message;
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
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::org_ns;
use crate::team;
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

    fn get_by_id_(
        &self,
        tx: &Transaction<TransactionDB>,
        organization_id: u64,
        team_id: u64,
    ) -> Result<Team> {
        let key = make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("team {team_id} not found").to_string(),
            )),
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
            data,
        )?;

        insert_index(&tx, idx_keys.as_ref(), team.id)?;
        tx.commit()?;
        Ok(team)
    }

    pub fn get_by_id(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, organization_id, team_id)
    }

    pub fn list(&self, organization_id: u64) -> Result<ListResponse<Team>> {
        let tx = self.db.transaction();

        let prefix = crate::make_data_key(org_ns(organization_id, NAMESPACE).as_slice());

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if key.len() < prefix.len() || !prefix.as_slice().cmp(&key[..prefix.len()]).is_eq() {
                continue;
            }
            list.push(deserialize(&value)?);
            break;
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update(
        &self,
        organization_id: u64,
        team_id: u64,
        req: UpdateTeamRequest,
    ) -> Result<Team> {
        let tx = self.db.transaction();

        let prev_team = self.get_by_id_(&tx, organization_id, team_id)?;
        let mut team = prev_team.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(organization_id, name.as_str()));
            idx_prev_keys.push(index_name_key(organization_id, prev_team.name.as_str()));
            team.name.clone_from(name);
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        team.updated_at = Some(Utc::now());
        team.updated_by = Some(req.updated_by);

        let data = serialize(&team)?;
        tx.put(
            make_data_value_key(org_ns(organization_id, NAMESPACE).as_slice(), team_id),
            data,
        )?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), team_id)?;
        tx.commit()?;
        Ok(team)
    }

    pub fn delete(&self, organization_id: u64, team_id: u64) -> Result<Team> {
        let tx = self.db.transaction();

        let team = self.get_by_id_(&tx, organization_id, team_id)?;
        tx.delete(make_data_value_key(
            org_ns(organization_id, NAMESPACE).as_slice(),
            team_id,
        ))?;

        delete_index(&tx, index_keys(organization_id, &team.name).as_ref())?;
        tx.commit()?;
        Ok(team)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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

// serialize team to protobuf
fn serialize(team: &Team) -> Result<Vec<u8>> {
    let v = team::Team {
        id: team.id,
        created_at: team.created_at.timestamp(),
        updated_at: team.updated_at.map(|t| t.timestamp()),
        created_by: team.created_by,
        updated_by: team.updated_by,
        organization_id: team.organization_id,
        name: team.name.clone(),
    };

    Ok(v.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Team> {
    let from = team::Team::decode(data)?;

    Ok(Team {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        updated_at: from
            .updated_at
            .map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        created_by: from.created_by,
        updated_by: from.updated_by,
        organization_id: from.organization_id,
        name: from.name,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_roundtrip() {
        use chrono::DateTime;

        use crate::teams::deserialize;
        use crate::teams::serialize;
        use crate::teams::Team;

        let team = Team {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            created_by: 1,
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            updated_by: Some(2),
            organization_id: 1,
            name: "test".to_string(),
        };

        let data = serialize(&team).unwrap();
        let team2 = deserialize(&data).unwrap();

        assert_eq!(team, team2);
    }
}
