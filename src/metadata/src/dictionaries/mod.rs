pub mod provider_impl;

use std::hash::Hash;
use async_trait::async_trait;
pub use provider_impl::ProviderImpl;

use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send + Hash {
    async fn get_key_or_create(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        value: &str,
    ) -> Result<u64>;
    async fn get_value(
        &self,
        organization_id: u64,
        project_id: u64,
        dict: &str,
        key: u64,
    ) -> Result<String>;
    async fn get_key(
        &self,
        _organization_id: u64,
        _project_id: u64,
        _dict: &str,
        value: &str,
    ) -> Result<u64>;
}
