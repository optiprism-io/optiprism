use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use common::rbac::OrganizationRole;
use crate::accounts::Accounts;
use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::{make_data_key, organization};
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::{ListResponse, ResponseMetadata};
use crate::Result;

const NAMESPACE: &[u8] = b"organizations";
const IDX_NAME: &[u8] = b"name";

fn index_keys(name: &str) -> Vec<Option<Vec<u8>>> {
    [index_name_key(name)].to_vec()
}

fn index_name_key(name: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_NAME, name).to_vec())
}

pub struct Organizations {
    db: Arc<TransactionDB>,
    accs: Arc<Accounts>,
}

impl Organizations {
    pub fn new(db: Arc<TransactionDB>, accs: Arc<Accounts>) -> Self {
        Organizations { db, accs }
    }

    fn get_by_id_(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Organization> {
        let key = make_data_value_key(NAMESPACE, id);

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("organization {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, req: CreateOrganizationRequest) -> Result<Organization> {
        let tx = self.db.transaction();

        let idx_keys = index_keys(&req.name);
        check_insert_constraints(&tx, idx_keys.as_ref())?;

        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let org = Organization {
            id,
            created_at,
            created_by: req.created_by,
            updated_at: None,
            updated_by: None,
            name: req.name,
            members: vec![(req.created_by, OrganizationRole::Owner)],
        };

        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, id), data)?;

        insert_index(&tx, idx_keys.as_ref(), org.id)?;

        self.accs.add_organization_(&tx, req.created_by, id, OrganizationRole::Owner)?;
        tx.commit()?;
        Ok(org)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Organization> {
        let tx = self.db.transaction();

        self.get_by_id_(&tx, id)
    }

    pub fn list(&self) -> Result<ListResponse<Organization>> {
        let tx = self.db.transaction();

        let prefix = make_data_key(NAMESPACE);
        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix).unwrap().is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update_(&self, tx: &Transaction<TransactionDB>, id: u64, req: UpdateOrganizationRequest) -> Result<Organization> {
        let prev_org = self.get_by_id_(tx, id)?;

        let mut org = prev_org.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(name) = &req.name {
            idx_keys.push(index_name_key(name.as_str()));
            idx_prev_keys.push(index_name_key(prev_org.name.as_str()));
            org.name.clone_from(name);
        }

        check_update_constraints(tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        org.updated_at = Some(Utc::now());
        org.updated_by = Some(req.updated_by);

        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, org.id), data)?;

        update_index(tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), org.id)?;
        Ok(org)
    }

    pub fn update(&self, id: u64, req: UpdateOrganizationRequest) -> Result<Organization> {
        let tx = self.db.transaction();
        let org = self.update_(&tx, id, req)?;
        tx.commit()?;
        Ok(org)
    }

    pub fn add_member(&self, id: u64, member_id: u64, role: OrganizationRole) -> Result<()> {
        let tx = self.db.transaction();
        let mut org = self.get_by_id_(&tx, id)?;
        if org.members.iter().any(|(id, _)| *id == member_id) {
            return Err(MetadataError::AlreadyExists(
                format!("member {member_id} already exists").to_string(),
            ));
        }
        org.members.push((member_id, role.clone()));

        self.accs.add_organization_(&tx, member_id, id, role)?;
        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, org.id), data)?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_member(&self, id: u64, member_id: u64) -> Result<()> {
        let tx = self.db.transaction();
        let mut org = self.get_by_id_(&tx, id)?;
        if !org.members.iter().any(|(id, _)| *id == member_id) {
            return Err(MetadataError::NotFound(
                format!("member {member_id} not found").to_string(),
            ));
        }
        org.members.retain(|(id, _)| *id != member_id);

        self.accs.remove_organization_(&tx, member_id, id)?;
        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, org.id), data)?;
        tx.commit()?;
        Ok(())
    }

    pub fn change_member_role(&self, id: u64, member_id: u64, role: OrganizationRole) -> Result<()> {
        let tx = self.db.transaction();
        let mut org = self.get_by_id_(&tx, id)?;
        if let Some(member) = org.members.iter_mut().find(|(id, _)| *id == member_id) {
            member.1 = role.clone();
            self.accs.change_organization_role_(&tx, member_id, id, role)?;
        } else {
            return Err(MetadataError::NotFound(
                format!("member {member_id} not found").to_string(),
            ));
        }

        let data = serialize(&org)?;
        tx.put(make_data_value_key(NAMESPACE, org.id), data)?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, id: u64) -> Result<Organization> {
        let tx = self.db.transaction();

        let org = self.get_by_id_(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        delete_index(&tx, index_keys(&org.name).as_ref())?;
        tx.commit()?;
        Ok(org)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Organization {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub name: String,
    pub members: Vec<(u64, OrganizationRole)>,
}

impl Organization {
    pub fn is_member(&self, member_id: u64) -> bool {
        self.members.iter().any(|(id, _)| *id == member_id)
    }
}
#[derive(Serialize, Deserialize, Clone)]
pub struct CreateOrganizationRequest {
    pub created_by: u64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct UpdateOrganizationRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
}

// serialize into protobuf
fn serialize(org: &Organization) -> Result<Vec<u8>> {
    let v = organization::Organization {
        id: org.id,
        created_at: org.created_at.timestamp(),
        created_by: org.created_by,
        updated_at: org.updated_at.map(|t| t.timestamp()),
        updated_by: org.updated_by,
        name: org.name.clone(),
        members: org
            .members
            .iter()
            .map(|(id, role)| organization::Member {
                id: *id,
                role: role.clone() as i32,
            })
            .collect(),
    };

    Ok(v.encode_to_vec())
}

// deserialize from protobuf
fn deserialize(data: &[u8]) -> Result<Organization> {
    let from = organization::Organization::decode(data)?;

    Ok(Organization {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        name: from.name,
        members: from
            .members
            .iter()
            .map(|m| (m.id, match m.role {
                1 => OrganizationRole::Owner,
                2 => OrganizationRole::Admin,
                3 => OrganizationRole::Member,
                _ => unreachable!()
            }))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use common::rbac::OrganizationRole;
    use crate::organizations::{Organization, deserialize, serialize};

    #[test]
    fn test_roundtrip() {
        let org = Organization {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            created_by: 1,
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            updated_by: Some(2),
            name: "test".to_string(),
            members: vec![(1, OrganizationRole::Owner)],
        };

        let data = serialize(&org).unwrap();
        let org2 = deserialize(&data).unwrap();

        assert_eq!(org, org2);
    }
}