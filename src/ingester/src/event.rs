use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;

#[derive(Debug, Deserialize)]
pub struct Context {
    pub library: Library,
    pub page: Option<Page>,
    #[serde(rename = "userAgent")]
    pub user_agent: String,
    pub ip: Ipv4Addr,
}

#[derive(Debug, Deserialize)]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct Page {
    pub path: String,
    pub referrer: String,
    pub search: String,
    pub title: String,
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Float64(f64),
    Bool(bool),
}


pub struct Property {
    pub id: u64,
    pub name: String,
    pub value: Option<PropValue>,
}

pub struct Event {
    pub name: String,
    pub properties: Vec<Property>,
}

pub struct User {
    pub id: u64,
    pub id_str: String,
    pub properties: Vec<Property>,
}
