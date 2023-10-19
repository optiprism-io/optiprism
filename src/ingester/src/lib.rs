use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;

pub mod destination;
pub mod destinations;
pub mod error;
pub mod executor;
pub mod processor;
pub mod processors;
pub mod sources;
pub mod track;

pub struct AppContext {
    project_id: u64,
    organization_id: u64,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: Option<Ipv4Addr>,
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
pub struct Property {
    pub id: u64,
    pub value: PropValue,
}

#[derive(Debug, Clone)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Number(Decimal),
    Bool(bool),
}
