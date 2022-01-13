pub mod local;
pub mod types;

use crate::Result;
use async_trait::async_trait;
use types::{Account, CreateRequest, ListRequest, UpdateRequest};

#[async_trait]
pub trait Provider {
    async fn create(&self, request: CreateRequest) -> Result<Account>;
    async fn get(&self, id: u64) -> Result<Option<Account>>;
    async fn list(&self, request: ListRequest) -> Result<Vec<Account>>;
    async fn update(&self, request: UpdateRequest) -> Result<Account>;
    async fn delete(&self, id: u64) -> Result<()>;
}
