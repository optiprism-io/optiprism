use std::string::FromUtf8Error;
use std::{error, result};

use crate::database::{Column, TableRef};
use crate::properties;
use thiserror::Error;

pub type Result<T> = result::Result<T, MetadataError>;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("column already exist: {0:?}")]
    ColumnAlreadyExists(Column),
    #[error("table not found: {0:?}")]
    TableNotFound(TableRef),
    #[error("table already exist: {0:?}")]
    TableAlreadyExists(TableRef),
}

#[derive(Error, Debug)]
pub enum EventError {
    #[error("event not found: {0:?}")]
    EventNotFound(Event),
    #[error("event already exist: {0:?}")]
    EventAlreadyExist(Event),
    #[error("property not found: {0:?}")]
    PropertyNotFound(Property),
    #[error("property already exist: {0:?}")]
    PropertyAlreadyExist(Property),
}

#[derive(Debug)]
pub struct Event {
    organization_id: u64,
    project_id: u64,
    event_id: Option<u64>,
    event_name: Option<String>,
}

#[derive(Error, Debug)]
pub enum CustomEventError {
    #[error("event not found: {0:?}")]
    EventNotFound(CustomEvent),
    #[error("event already exist: {0:?}")]
    EventAlreadyExist(CustomEvent),
    #[error("recursion level {0} exceeded")]
    RecursionLevelExceeded(usize),
    #[error("duplicate event")]
    DuplicateEvent,
    #[error("empty events")]
    EmptyEvents,
}

#[derive(Debug)]
pub struct CustomEvent {
    pub organization_id: u64,
    pub project_id: u64,
    pub event_id: Option<u64>,
    pub event_name: Option<String>,
}

impl CustomEvent {
    pub fn new_with_name(organization_id: u64, project_id: u64, event_name: String) -> Self {
        Self {
            organization_id,
            project_id,
            event_id: None,
            event_name: Some(event_name),
        }
    }

    pub fn new_with_id(organization_id: u64, project_id: u64, event_id: u64) -> Self {
        Self {
            organization_id,
            project_id,
            event_id: Some(event_id),
            event_name: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum AccountError {
    #[error("account not found: {0:?}")]
    AccountNotFound(Account),
    #[error("account already exist: {0:?}")]
    AccountAlreadyExist(Account),
}

#[derive(Debug)]
pub struct Account {
    account_id: Option<u64>,
    email: Option<String>,
}

impl Account {
    pub fn new_with_email(email: String) -> Self {
        Self {
            account_id: None,
            email: Some(email),
        }
    }

    pub fn new_with_id(account_id: u64) -> Self {
        Self {
            account_id: Some(account_id),
            email: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum OrganizationError {
    #[error("organization not found: {0:?}")]
    OrganizationNotFound(Organization),
    #[error("organization already exist: {0:?}")]
    OrganizationAlreadyExist(Organization),
}

#[derive(Debug)]
pub struct Organization {
    id: Option<u64>,
    name: Option<String>,
}

impl Organization {
    pub fn new_with_id(id: u64) -> Self {
        Self { id: Some(id), name: None }
    }
    pub fn new_with_name(name: String) -> Self {
        Self { id: None, name: Some(name) }
    }
}

#[derive(Error, Debug)]
pub enum ProjectError {
    #[error("project not found: {0:?}")]
    ProjectNotFound(Project),
    #[error("project already exist: {0:?}")]
    ProjectAlreadyExist(Project),
}

#[derive(Debug)]
pub struct Project {
    organization_id: u64,
    id: Option<u64>,
    name: Option<String>,
}

impl Project {
    pub fn new_with_name(organization_id: u64, name: String) -> Self {
        Self {
            organization_id,
            id: None,
            name: Some(name),
        }
    }

    pub fn new_with_id(organization_id: u64, id: u64) -> Self {
        Self {
            organization_id,
            id: Some(id),
            name: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum TeamError {
    #[error("team not found: {0:?}")]
    TeamNotFound(Team),
    #[error("team already exist: {0:?}")]
    TeamAlreadyExist(Team),
}

#[derive(Debug)]
pub struct Team {
    organization_id: u64,
    id: Option<u64>,
    name: Option<String>,
}

impl Team {
    pub fn new_with_name(organization_id: u64, name: String) -> Self {
        Self {
            organization_id,
            id: None,
            name: Some(name),
        }
    }

    pub fn new_with_id(organization_id: u64, id: u64) -> Self {
        Self {
            organization_id,
            id: Some(id),
            name: None,
        }
    }
}

#[derive(Debug)]
pub struct Property {
    pub organization_id: u64,
    pub project_id: u64,
    pub namespace: properties::provider::Namespace,
    pub event_id: Option<u64>,
    pub property_id: Option<u64>,
    pub property_name: Option<String>,
}

impl Event {
    pub fn new_with_name(organization_id: u64, project_id: u64, event_name: String) -> Self {
        Self {
            organization_id,
            project_id,
            event_id: None,
            event_name: Some(event_name),
        }
    }

    pub fn new_with_id(organization_id: u64, project_id: u64, event_id: u64) -> Self {
        Self {
            organization_id,
            project_id,
            event_id: Some(event_id),
            event_name: None,
        }
    }
}

#[derive(Error, Debug)]
pub enum PropertyError {
    #[error("property not found: {0:?}")]
    PropertyNotFound(Property),
    #[error("property already exist: {0:?}")]
    PropertyAlreadyExist(Property),
}

#[derive(Error, Debug)]
pub enum DictionaryError {
    #[error("key not found: {0:?}")]
    KeyNotFound(DictionaryKey),
    #[error("value not found: {0:?}")]
    ValueNotFound(DictionaryValue),
}

#[derive(Debug)]
pub struct DictionaryKey {
    organization_id: u64,
    project_id: u64,
    dict: String,
    key: u64,
}

impl DictionaryKey {
    pub fn new(organization_id: u64, project_id: u64, dict: String, key: u64) -> Self {
        Self {
            organization_id,
            project_id,
            dict,
            key,
        }
    }
}

#[derive(Debug)]
pub struct DictionaryValue {
    organization_id: u64,
    project_id: u64,
    dict: String,
    value: String,
}

impl DictionaryValue {
    pub fn new(organization_id: u64, project_id: u64, dict: String, value: String) -> Self {
        Self {
            organization_id,
            project_id,
            dict,
            value,
        }
    }
}

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("key already exist: {0:?}")]
    KeyAlreadyExists(String),
    #[error("key not found: {0:?}")]
    KeyNotFound(String),
}

#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("database {0:?}")]
    Database(#[from] DatabaseError),
    #[error("account {0:?}")]
    Account(#[from] AccountError),
    #[error("organization {0:?}")]
    Organization(#[from] OrganizationError),
    #[error("project {0:?}")]
    Project(#[from] ProjectError),
    #[error("team {0:?}")]
    Team(#[from] TeamError),
    #[error("event {0:?}")]
    Event(#[from] EventError),
    #[error("custom event {0:?}")]
    CustomEvent(#[from] CustomEventError),
    #[error("property {0:?}")]
    Property(#[from] PropertyError),
    #[error("dictionary {0:?}")]
    Dictionary(#[from] DictionaryError),
    #[error("store {0:?}")]
    Store(#[from] StoreError),
    #[error("rocksdb {0:?}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("from utf {0:?}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("bincode {0:?}")]
    Bincode(#[from] bincode::Error),
    #[error("io {0}")]
    Io(#[from] std::io::Error),
    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
}
