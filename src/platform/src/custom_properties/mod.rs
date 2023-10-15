mod provider_impl;

use axum::async_trait;
use chrono::DateTime;
use chrono::Utc;
pub use provider_impl::ProviderImpl;
use serde::Deserialize;
use serde::Serialize;

use crate::Context;
use crate::ListResponse;
use crate::PlatformError;
use crate::Result;

#[async_trait]
pub trait Provider: Sync + Send {
    async fn list(
        &self,
        ctx: Context,
        organization_id: u64,
        project_id: u64,
    ) -> Result<ListResponse<CustomProperty>>;
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    #[default]
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CustomProperty {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub description: Option<String>,
}

impl TryInto<metadata::custom_properties::CustomProperty> for CustomProperty {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<metadata::custom_properties::CustomProperty, Self::Error> {
        Ok(metadata::custom_properties::CustomProperty {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
        })
    }
}

impl TryInto<CustomProperty> for metadata::custom_properties::CustomProperty {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<CustomProperty, Self::Error> {
        Ok(CustomProperty {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
            created_by: self.created_by,
            updated_by: self.updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            description: self.description,
        })
    }
}
