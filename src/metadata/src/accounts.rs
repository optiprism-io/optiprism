use std::sync::Arc;

use bincode::deserialize;
use bincode::serialize;
use chrono::DateTime;
use chrono::Utc;
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
use crate::list_data;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::make_index_key;
use crate::metadata::ListResponse;
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
        list_data(&tx, NAMESPACE)
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
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: Option<u64>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub password_hash: String,
    pub email: String,
    pub name: Option<String>,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateAccountRequest {
    pub created_by: Option<u64>,
    pub password_hash: String,
    pub email: String,
    pub name: Option<String>,
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
            role: self.role,
            organizations: self.organizations,
            projects: self.projects,
            teams: self.teams,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct UpdateAccountRequest {
    pub updated_by: u64,
    pub password_hash: OptionalProperty<String>,
    pub email: OptionalProperty<String>,
    pub name: OptionalProperty<Option<String>>,
    pub role: OptionalProperty<Option<Role>>,
    pub organizations: OptionalProperty<Option<Vec<(u64, OrganizationRole)>>>,
    pub projects: OptionalProperty<Option<Vec<(u64, ProjectRole)>>>,
    pub teams: OptionalProperty<Option<Vec<(u64, Role)>>>,
}
