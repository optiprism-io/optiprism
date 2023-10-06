//! Provider to handle counters, like sequential IDs for primary keys.

pub mod provider_impl;

use async_trait::async_trait;

use crate::Result;
pub use provider_impl::ProviderImpl;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn next_event_record(&self, organization_id: u64, project_id: u64) -> Result<u64>;
}
