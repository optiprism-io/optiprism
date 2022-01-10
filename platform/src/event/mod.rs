pub mod provider_impl;

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use super::error::Result;

#[async_trait]
pub trait Provider {
    async fn create_event(&self, event: Event) -> Result<Event>;
    async fn update_event(&self, event: Event) -> Result<Event>;
    async fn get_event(&self, id: u64) -> Result<Option<Event>>;
    async fn delete_event(&self, id: u64) -> Result<()>;
    async fn list_events(&self) -> Result<Vec<Event>>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub name: String,
}
