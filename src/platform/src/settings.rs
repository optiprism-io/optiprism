use std::sync::Arc;

use common::rbac::Permission;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::Context;

pub struct SettingsProvider {
    prov: Arc<metadata::settings::SettingsProvider>,
}

impl SettingsProvider {
    pub fn new(prov: Arc<metadata::settings::SettingsProvider>) -> Self {
        Self { prov }
    }

    pub async fn set(&self, ctx: Context, req: Settings) -> Result<Settings> {
        ctx.check_permission(Permission::ManageServer)?;

        let mut settings = self.prov.load()?;

        if let Some(v) = req.auth_access_token {
            settings.auth_access_token = v;
        }
        if let Some(v) = req.auth_refresh_token {
            settings.auth_refresh_token = v;
        }
        if let Some(v) = req.backup_enabled {
            settings.backup_enabled = v;
        }
        if let Some(v) = req.backup_encryption_enabled {
            settings.backup_encryption_enabled = v;
        }
        if let Some(v) = req.backup_encryption_password {
            settings.backup_encryption_password = v;
        }
        if let Some(v) = req.backup_compression_enabled {
            settings.backup_compression_enabled = v;
        }
        if let Some(v) = req.backup_provider {
            match v {
                BackupProvider::Local => {
                    settings.backup_provider = metadata::settings::BackupProvider::Local
                }
                BackupProvider::S3 => {
                    settings.backup_provider = metadata::settings::BackupProvider::S3
                }
                BackupProvider::Gcp => {
                    settings.backup_provider = metadata::settings::BackupProvider::GCP
                }
            }
        }
        if let Some(v) = req.backup_provider_s3_bucket {
            settings.backup_provider_s3_bucket = v;
        }
        if let Some(v) = req.backup_provider_s3_path {
            settings.backup_provider_s3_path = v;
        }
        if let Some(v) = req.backup_provider_s3_region {
            settings.backup_provider_s3_region = v;
        }
        if let Some(v) = req.backup_provider_s3_access_key {
            settings.backup_provider_s3_access_key = v;
        }
        if let Some(v) = req.backup_provider_s3_secret_key {
            settings.backup_provider_s3_secret_key = v;
        }
        if let Some(v) = req.backup_provider_gcp_bucket {
            settings.backup_provider_gcp_bucket = v;
        }
        if let Some(v) = req.backup_provider_gcp_path {
            settings.backup_provider_gcp_path = v;
        }

        self.prov.save(&settings)?;

        self.get(ctx).await
    }

    pub async fn get(&self, ctx: Context) -> Result<Settings> {
        ctx.check_permission(Permission::ManageServer)?;

        let settings = self.prov.load()?;

        let ret = Settings {
            auth_access_token: None,
            auth_refresh_token: None,
            auth_admin_default_password: None,
            backup_enabled: Some(settings.backup_enabled),
            backup_encryption_enabled: Some(settings.backup_encryption_enabled),
            backup_encryption_password: None,
            backup_compression_enabled: Some(settings.backup_compression_enabled),
            backup_provider: match settings.backup_provider {
                metadata::settings::BackupProvider::Local => Some(BackupProvider::Local),
                metadata::settings::BackupProvider::S3 => Some(BackupProvider::S3),
                metadata::settings::BackupProvider::GCP => Some(BackupProvider::Gcp),
            },
            backup_provider_s3_bucket: Some(settings.backup_provider_s3_bucket),
            backup_provider_s3_path: Some(settings.backup_provider_s3_path),
            backup_provider_s3_region: Some(settings.backup_provider_s3_region),
            backup_provider_s3_access_key: Some(settings.backup_provider_s3_access_key),
            backup_provider_s3_secret_key: None,
            backup_provider_gcp_bucket: Some(settings.backup_provider_gcp_bucket),
            backup_provider_gcp_path: Some(settings.backup_provider_gcp_path),
            backup_provider_gcp_key: None,
            backup_schedule_interval: Some(match settings.backup_schedule_interval {
                metadata::settings::BackupScheduleInterval::Hourly => {
                    BackupScheduleInterval::Hourly
                }
                metadata::settings::BackupScheduleInterval::Daily => BackupScheduleInterval::Daily,
                metadata::settings::BackupScheduleInterval::Weekly => {
                    BackupScheduleInterval::Weekly
                }
                metadata::settings::BackupScheduleInterval::Monthly => {
                    BackupScheduleInterval::Monthly
                }
                metadata::settings::BackupScheduleInterval::Yearly => {
                    BackupScheduleInterval::Yearly
                }
            }),
            backup_schedule_start_hour: Some(settings.backup_schedule_start_hour),
        };

        Ok(ret)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BackupProvider {
    Local,
    S3,
    #[serde(rename = "gcp")] // for some reason it is "gCP"
    Gcp,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum BackupScheduleInterval {
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Yearly,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_admin_default_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_encryption_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_encryption_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_compression_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider: Option<BackupProvider>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_s3_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_s3_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_s3_region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_s3_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_s3_secret_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_gcp_bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_gcp_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_provider_gcp_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_schedule_interval: Option<BackupScheduleInterval>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_schedule_start_hour: Option<usize>,
}
