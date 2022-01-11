mod event;
mod error;
mod kv;

use async_trait::async_trait;
use crate::event::Event;
use crate::error::Result;

#[async_trait]
pub trait EventProvider {
    async fn create_event(&self, event: Event) -> Result<Event>;
    async fn update_event(&self, event: Event) -> Result<Event>;
    async fn get_event(&self, id: u64) -> Result<Option<Event>>;
    async fn delete_event(&self, id: u64) -> Result<()>;
    async fn list_events(&self) -> Result<Vec<Event>>;
}