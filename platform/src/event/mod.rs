pub mod provider_impl;

use super::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub trait Provider {
    async fn create_event(&self, event: Event) -> Result<Event>;
    async fn update_event(&self, event: Event) -> Result<Event>;
    async fn get_event(&self, id: u64) -> Result<Option<Event>>;
    async fn delete_event(&self, id: u64) -> Result<()>;
    async fn list_events(&self) -> Result<Vec<Event>>;
}

#[derive(Serialize, Deserialize, Clone)]
enum Status {
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
