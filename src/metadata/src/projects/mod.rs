pub mod provider_impl;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use common::types::OptionalProperty;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::metadata::ListResponse;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn create(&self, organization_id: u64, req: CreateProjectRequest) -> Result<Project>;
    async fn get_by_id(&self, organization_id: u64, project_id: u64) -> Result<Project>;
    async fn get_by_token(&self, token: &str) -> Result<Project>;
    async fn list(&self, organization_id: u64) -> Result<ListResponse<Project>>;
    async fn update(
        &self,
        organization_id: u64,
        project_id: u64,
        req: UpdateProjectRequest,
    ) -> Result<Project>;
    async fn delete(&self, organization_id: u64, project_id: u64) -> Result<Project>;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum SDKLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SDK {
    pub autotrack_pageviews: bool,
    pub log_level: SDKLogLevel,
}

impl ToString for SDKLogLevel {
    fn to_string(&self) -> String {
        match self {
            SDKLogLevel::Debug => "Debug",
            SDKLogLevel::Info => "Info",
            SDKLogLevel::Warn => "Warn",
            SDKLogLevel::Error => "Error",
        }
        .to_string()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Project {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: u64,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_by: Option<u64>,
    pub organization_id: u64,
    pub name: String,
    pub token: String,
    pub sdk: SDK,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CreateProjectRequest {
    pub created_by: u64,
    pub name: String,
    pub sdk: SDK,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateProjectRequest {
    pub updated_by: u64,
    pub name: OptionalProperty<String>,
    pub sdk: OptionalProperty<SDK>,
}
