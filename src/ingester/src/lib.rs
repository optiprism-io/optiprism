use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;
use error::Result;
use metadata::events;
use metadata::properties;
use rust_decimal::Decimal;

pub mod destinations;
pub mod error;
pub mod executor;
pub mod sources;
pub mod transformers;

pub trait Transformer<T>: Send + Sync {
    fn process(&self, ctx: &RequestContext, req: T) -> Result<T>;
}

pub trait Destination<T>: Send + Sync {
    fn send(&self, ctx: &RequestContext, req: T) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    project_id: Option<u64>,
    organization_id: Option<u64>,
    client_ip: IpAddr,
    token: String,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: Option<IpAddr>,
}

#[derive(Debug, Clone)]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub path: Option<String>,
    pub referrer: Option<String>,
    pub search: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PropertyAndValue {
    pub property: properties::Property,
    pub value: PropValue,
}
#[derive(Debug, Clone)]
pub struct Event {
    pub record_id: u64,
    pub event: events::Event,
}

#[derive(Debug, Clone)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Number(Decimal),
    Bool(bool),
}

#[derive(Debug, Clone)]
pub struct Identify {
    pub user_id: Option<String>,
    pub resolved_user_id: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    pub event: String,
    pub user_properties: Option<HashMap<String, PropValue>>,
    pub resolved_user_properties: Option<Vec<PropertyAndValue>>,
}

#[derive(Debug, Clone)]
pub struct Track {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub resolved_user_id: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
    pub context: Context,
    pub event: String,
    pub resolved_event: Option<Event>,
    pub properties: Option<HashMap<String, PropValue>>,
    pub user_properties: Option<HashMap<String, PropValue>>,
    pub resolved_properties: Option<Vec<PropertyAndValue>>,
    pub resolved_user_properties: Option<Vec<PropertyAndValue>>,
}
