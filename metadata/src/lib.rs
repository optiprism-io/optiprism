mod error;
mod event;
mod kv;

use async_trait::async_trait;
pub use error::Result;
pub use event::Event;

#[async_trait]
pub trait EventProvider {
    async fn create_event(&self, event: Event) -> Result<Event>;
    async fn update_event(&self, event: Event) -> Result<Event>;
    async fn get_event(&self, id: u64) -> Result<Option<Event>>;
    async fn delete_event(&self, id: u64) -> Result<()>;
    async fn list_events(&self) -> Result<Vec<Event>>;
}
