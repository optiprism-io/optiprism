use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
pub struct EventWithContext {
    pub user_id: String,
    pub project_id: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub context: Option<Context>,
    pub event: Event,
}

#[derive(Serialize, Deserialize)]
pub enum PropertyValue {
    String(String),
    Float64(f64),
    Bool(bool),
    Date(u64),
}

#[derive(Serialize, Deserialize)]
pub struct Event {
    pub name: String,
    pub properties: Option<HashMap<String, PropertyValue>>,
}

#[derive(Serialize, Deserialize)]
pub struct Context {
    pub library: Library,
    pub page: Option<Page>,
    pub browser: Option<Browser>,
    pub app: Option<App>,
    pub ip: String,
}

#[derive(Serialize, Deserialize)]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Serialize, Deserialize)]
pub struct Page {
    pub location: String,
    pub referrer: String,
    pub title: String,
}

#[derive(Serialize, Deserialize)]
pub struct Browser {
    pub user_agent: String,
    pub device_memory: f64,
    pub screen_width: u64,
    pub screen_height: u64,
    pub client_hints: Option<ClientHints>,
}

#[derive(Serialize, Deserialize)]
pub struct ClientHints {
    pub architecture: String,
    pub bitness: String,
    pub brand: String,
    pub brand_version: String,
    pub mobile: bool,
    pub model: String,
    pub platform: String,
    pub platform_version: String,
}

#[derive(Serialize, Deserialize)]
pub struct App {
    pub id: String,
}
