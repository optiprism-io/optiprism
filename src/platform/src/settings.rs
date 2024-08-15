use std::sync::Arc;
use serde::{Deserialize, Serialize};
use metadata::settings::Settings as MDSettings;
use crate::error::Result;

pub struct SettingsProvider {
    prov: Arc<MDSettings>,
}

impl SettingsProvider {
    pub fn new(prov: Arc<MDSettings>) -> Self {
        Self { prov }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum BackupProvider {
    Local,
    S3,
    GCP,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum BackupScheduleInterval {
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Yearly,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Settings {
    pub auth_access_token: Option<String>,
    pub auth_refresh_token: Option<String>,
    pub auth_admin_default_password: Option<String>,
    pub backup_enabled: Option<bool>,
    pub backup_encryption_enabled: Option<bool>,
    pub backup_encryption_password: Option<String>,
    pub backup_compression_enabled: Option<bool>,
    pub backup_provider: Option<BackupProvider>,
    pub backup_provider_s3_bucket: Option<String>,
    pub backup_provider_s3_path: Option<String>,
    pub backup_provider_s3_region: Option<String>,
    pub backup_provider_s3_access_key: Option<String>,
    pub backup_provider_s3_secret_key: Option<String>,
    pub backup_provider_gcp_bucket: Option<String>,
    pub backup_provider_gcp_path: Option<String>,
    pub backup_provider_gcp_key: Option<String>,
    pub backup_schedule_interval: Option<BackupScheduleInterval>,
    pub backup_schedule_start_hour: Option<usize>,
}