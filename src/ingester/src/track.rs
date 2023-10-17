use std::collections::HashMap;
use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct Track {
    pub user_id: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
    pub context: Option<Context>,
    pub event: String,
    pub raw_properties: HashMap<String, PropValue>,
    pub raw_user_properties: HashMap<String, PropValue>,
    pub properties: Option<Vec<Property>>,
    pub user_properties: Option<Vec<Property>>,
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
