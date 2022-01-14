use std::sync::Arc;
use crate::{
    kv::{self, KV},
    Result,
};
use bincode::{deserialize, serialize};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub enum Status {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub update_by: u64,
    pub project_id: u64,
    pub is_system: bool,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}