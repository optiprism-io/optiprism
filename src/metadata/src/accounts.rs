use std::io::BufReader;
use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::OptionalProperty;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use crate::error::MetadataError;
use crate::index::check_insert_constraints;
use crate::index::check_update_constraints;
use crate::index::delete_index;
use crate::index::get_index;
use crate::index::insert_index;
use crate::index::next_seq;
use crate::index::update_index;
use crate::{account, list_data, make_data_key};
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::{ListResponse, ResponseMetadata};
use crate::Result;

const NAMESPACE: &[u8] = b"accounts";
const IDX_EMAIL: &[u8] = b"email";

fn index_keys(email: &str) -> Vec<Option<Vec<u8>>> {
    [index_email_key(email)].to_vec()
}

fn index_email_key(email: &str) -> Option<Vec<u8>> {
    Some(make_index_key(NAMESPACE, IDX_EMAIL, email).to_vec())
}

pub struct Accounts {
    db: Arc<TransactionDB>,
}

impl Accounts {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Accounts { db }
    }

    fn get_by_id_(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Account> {
        let key = make_data_value_key(NAMESPACE, id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("account {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, req: CreateAccountRequest) -> Result<Account> {
        let idx_keys = index_keys(&req.email);

        let tx = self.db.transaction();
        check_insert_constraints(&tx, idx_keys.as_ref())?;
        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let account = req.into_account(id, created_at);

        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        insert_index(&tx, idx_keys.as_ref(), account.id)?;
        tx.commit()?;
        Ok(account)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Account> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, id)
    }

    pub fn get_by_email(&self, email: &str) -> Result<Account> {
        let tx = self.db.transaction();
        let id = get_index(
            &tx,
            make_index_key(NAMESPACE, IDX_EMAIL, email),
            format!("account with email \"{}\" not found", email).as_str(),
        )?;
        self.get_by_id_(&tx, id)
    }

    pub fn list(&self) -> Result<ListResponse<Account>> {
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

    pub fn update(&self, account_id: u64, req: UpdateAccountRequest) -> Result<Account> {
        let tx = self.db.transaction();

        let prev_account = self.get_by_id_(&tx, account_id)?;
        let mut account = prev_account.clone();

        let mut idx_keys: Vec<Option<Vec<u8>>> = Vec::new();
        let mut idx_prev_keys: Vec<Option<Vec<u8>>> = Vec::new();
        if let OptionalProperty::Some(email) = &req.email {
            idx_keys.push(index_email_key(email.as_str()));
            idx_prev_keys.push(index_email_key(prev_account.email.as_str()));
            account.email = email.to_owned();
        }

        check_update_constraints(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref())?;

        account.updated_at = Some(Utc::now());
        account.updated_by = Some(req.updated_by);
        if let OptionalProperty::Some(name) = req.name {
            account.name = name;
        }
        if let OptionalProperty::Some(force_update_password) = req.force_update_password {
            account.force_update_password = force_update_password;
        }
        if let OptionalProperty::Some(organizations) = req.organizations {
            account.organizations = organizations;
        }
        if let OptionalProperty::Some(projects) = req.projects {
            account.projects = projects;
        }
        if let OptionalProperty::Some(teams) = req.teams {
            account.teams = teams;
        }
        if let OptionalProperty::Some(hash) = req.password_hash {
            account.password_hash = hash;
        }
        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        update_index(&tx, idx_keys.as_ref(), idx_prev_keys.as_ref(), account_id)?;
        tx.commit()?;
        Ok(account)
    }

    pub fn delete(&self, id: u64) -> Result<Account> {
        let tx = self.db.transaction();
        let account = self.get_by_id_(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        delete_index(&tx, index_keys(&account.email).as_ref())?;
        tx.commit()?;
        Ok(account)
    }

    pub(crate) fn add_organization_(&self, tx: &Transaction<TransactionDB>, member_id: u64, org_id: u64, role: OrganizationRole) -> Result<()> {
        let mut account = self.get_by_id_(&tx, member_id)?;
        let orgs = account.organizations.get_or_insert(Vec::new());
        if orgs.iter().any(|(id, _)| *id == org_id) {
            return Err(MetadataError::AlreadyExists(format!("member {member_id} already in organization {org_id}").to_string()));
        }
        orgs.push((org_id, role));
        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        Ok(())
    }

    pub(crate) fn remove_organization_(&self, tx: &Transaction<TransactionDB>, member_id: u64, org_id: u64) -> Result<()> {
        let mut account = self.get_by_id_(&tx, member_id)?;
        let orgs = account.organizations.get_or_insert(Vec::new());
        if orgs.iter().all(|(id, _)| *id != org_id) {
            return Err(MetadataError::NotFound(format!("member {member_id} not found in organization {org_id}").to_string()));
        }
        orgs.retain(|(id, _)| *id != org_id);
        let data = serialize(&account)?;
        tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;

        Ok(())
    }

    pub(crate) fn change_organization_role_(&self, tx: &Transaction<TransactionDB>, member_id: u64, org_id: u64, role: OrganizationRole) -> Result<()> {
        let mut account = self.get_by_id_(&tx, member_id)?;
        let orgs = account.organizations.get_or_insert(Vec::new());
        for (id, r) in orgs.iter_mut() {
            if *id == org_id {
                *r = role;
                let data = serialize(&account)?;
                tx.put(make_data_value_key(NAMESPACE, account.id), &data)?;
                return Ok(());
            }
        }
        Err(MetadataError::NotFound(format!("member {member_id} not found in organization {org_id}").to_string()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub password_hash: String,
    pub email: String,
    pub name: Option<String>,
    pub role: Option<Role>,
    pub force_update_password: bool,
    pub force_update_email: bool,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateAccountRequest {
    pub created_by: u64,
    pub password_hash: String,
    pub email: String,
    pub name: Option<String>,
    pub force_update_password: bool,
    pub force_update_email: bool,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl CreateAccountRequest {
    pub fn into_account(self, id: u64, created_at: DateTime<Utc>) -> Account {
        Account {
            id,
            created_at,
            created_by: self.created_by,
            updated_at: None,
            updated_by: None,
            password_hash: self.password_hash,
            email: self.email,
            name: self.name,
            force_update_password: self.force_update_password,
            role: self.role,
            organizations: self.organizations,
            projects: self.projects,
            teams: self.teams,
            force_update_email: self.force_update_email,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateAccountRequest {
    pub updated_by: u64,
    pub password_hash: OptionalProperty<String>,
    pub email: OptionalProperty<String>,
    pub name: OptionalProperty<Option<String>>,
    pub force_update_password: OptionalProperty<bool>,
    pub force_update_email: OptionalProperty<bool>,
    pub role: OptionalProperty<Option<Role>>,
    pub organizations: OptionalProperty<Option<Vec<(u64, OrganizationRole)>>>,
    pub projects: OptionalProperty<Option<Vec<(u64, ProjectRole)>>>,
    pub teams: OptionalProperty<Option<Vec<(u64, Role)>>>,
}

fn serialize(acc: &Account) -> Result<Vec<u8>> {
    let role = if let Some(role) = &acc.role {
        match role {
            Role::Admin => Some(account::Role::Admin as i32),
        }
    } else { None };

    let orgs = if let Some(orgs) = &acc.organizations {
        orgs.iter().map(|(id, role)| {
            account::Organization {
                id: *id,
                role: match role {
                    OrganizationRole::Owner => account::OrganizationRole::Owner as i32,
                    OrganizationRole::Admin => account::OrganizationRole::Admin as i32,
                    OrganizationRole::Member => account::OrganizationRole::Member as i32,
                },
            }
        }).collect::<Vec<_>>()
    } else {
        vec![]
    };

    let proj = if let Some(proj) = &acc.projects {
        proj.iter().map(|(id, role)| {
            account::Project {
                id: *id,
                role: match role {
                    ProjectRole::Owner => account::ProjectRole::Owner as i32,
                    ProjectRole::Admin => account::ProjectRole::Admin as i32,
                    ProjectRole::Member => account::ProjectRole::Member as i32,
                    ProjectRole::Reader => account::ProjectRole::Reader as i32,
                },
            }
        }).collect::<Vec<_>>()
    } else {
        vec![]
    };

    let teams = if let Some(teams) = &acc.teams {
        teams.iter().map(|(id, role)| {
            account::Team {
                id: *id,
                role: match role {
                    Role::Admin => account::Role::Admin as i32,
                },
            }
        }).collect::<Vec<_>>()
    } else {
        vec![]
    };

    let acc = account::Account {
        id: acc.id,
        created_at: acc.created_at.timestamp(),
        created_by: acc.created_by,
        updated_at: acc.updated_at.map(|t| t.timestamp()),
        updated_by: acc.updated_by,
        password_hash: acc.password_hash.clone(),
        email: acc.email.clone(),
        name: acc.name.clone(),
        role,
        force_update_password: acc.force_update_password,
        force_update_email: acc.force_update_email,
        organizations: orgs,
        projects: proj,
        teams,
    };
    Ok(acc.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Account> {
    let from = account::Account::decode(data.as_ref())?;
    let role = if let Some(role) = &from.role {
        match role {
            1 => Some(Role::Admin),
            _ => panic!("invalid role")
        }
    } else {
        None
    };


    let orgs = from.organizations.iter().map(|org| {
        (org.id, match org.role {
            1 => OrganizationRole::Owner,
            2 => OrganizationRole::Admin,
            3 => OrganizationRole::Member,
            _ => panic!("invalid role")
        })
    }).collect::<Vec<_>>();

    let proj = from.projects.iter().map(|proj| {
        (proj.id, match proj.role {
            1 => ProjectRole::Owner,
            2 => ProjectRole::Admin,
            3 => ProjectRole::Member,
            4 => ProjectRole::Reader,
            _ => panic!("invalid role")
        })
    }).collect::<Vec<_>>();

    let teams = from.teams.iter().map(|team| {
        (team.id, match team.role {
            1 => Role::Admin,
            _ => panic!("invalid role")
        })
    }).collect::<Vec<_>>();
    let to = Account {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        created_by: from.created_by,
        updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        updated_by: from.updated_by,
        password_hash: from.password_hash,
        email: from.email,
        name: from.name,
        role,
        force_update_password: from.force_update_password,
        force_update_email: from.force_update_email,
        organizations: if orgs.is_empty() { None } else { Some(orgs) },
        projects: if proj.is_empty() { None } else { Some(proj) },
        teams: if teams.is_empty() { None } else { Some(teams) },
    };

    Ok(to)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use common::rbac::{OrganizationRole, ProjectRole, Role};
    use crate::accounts::{Account, deserialize, serialize};

    #[test]
    fn test_proto_roundtrip() {
        let acc = Account {
            id: 1,
            created_at: DateTime::from_timestamp(1, 0).unwrap(),
            created_by: 2,
            updated_at: Some(DateTime::from_timestamp(2, 0).unwrap()),
            updated_by: Some(3),
            password_hash: "4".to_string(),
            email: "5".to_string(),
            name: Some("6".to_string()),
            role: Some(Role::Admin),
            force_update_password: true,
            force_update_email: true,
            organizations: Some(vec![(1, OrganizationRole::Admin)]),
            projects: Some(vec![(1, ProjectRole::Admin)]),
            teams: Some(vec![(1, Role::Admin)]),
        };

        let s = serialize(&acc).unwrap();
        let d = deserialize(&s).unwrap();

        assert_eq!(acc, d);
    }
}