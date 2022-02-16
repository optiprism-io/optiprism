use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize)]
enum PropertyValue {
    String(String),
    Float64(f64),
    Bool(bool),
    Date(u64),
}

#[derive(Serialize, Deserialize)]
pub struct Request {
    user_id: String,
    created_at: Option<DateTime<Utc>>,
    context: Option<Context>,
    event: String,
    properties: Option<HashMap<String, PropertyValue>>,
}

#[derive(Serialize, Deserialize)]
pub struct Context {
    library: Library,
    page: Option<Page>,
    browser: Option<Browser>,
    app: Option<App>,
    ip: String,
}

#[derive(Serialize, Deserialize)]
pub struct Library {
    name: String,
    version: String,
}

#[derive(Serialize, Deserialize)]
pub struct Page {
    location: String,
    referrer: String,
    title: String,
}

#[derive(Serialize, Deserialize)]
pub struct Browser {
    user_agent: String,
    device_memory: f64,
    screen_width: u64,
    screen_height: u64,
    client_hints: Option<ClientHints>,
}

#[derive(Serialize, Deserialize)]
pub struct ClientHints {
    architecture: String,
    bitness: String,
    brand: String,
    brand_version: String,
    mobile: bool,
    model: String,
    platform: String,
    platform_version: String,
}

#[derive(Serialize, Deserialize)]
pub struct App {
    id: String,
}
