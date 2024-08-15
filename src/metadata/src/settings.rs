use std::path::PathBuf;
use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use chrono::Utc;
use prost::Message;
use rocksdb::{Transaction, TransactionDB};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use common::types::OptionalProperty;
use crate::error::MetadataError;
use crate::reports::{CreateReportRequest, Report, UpdateReportRequest};
use crate::{make_data_key, make_data_value_key, make_id_seq_key, pbconfig, project_ns, Result};
use crate::dashboards::Dashboard;
use crate::index::next_seq;
use crate::metadata::{ListResponse, ResponseMetadata};

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum BackupProvider {
    #[default]
    Local,
    S3,
    GCP,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum BackupScheduleInterval {
    #[default]
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Yearly,
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Settings {
    pub auth_access_token: String,
    pub auth_refresh_token: String,
    pub auth_admin_default_password: String,
    pub backup_enabled: bool,
    pub backup_encryption_enabled: bool,
    pub backup_encryption_password: String,
    pub backup_compression_enabled: bool,
    pub backup_provider: BackupProvider,
    pub backup_provider_local_path: String,
    pub backup_provider_s3_bucket: String,
    pub backup_provider_s3_path: String,
    pub backup_provider_s3_region: String,
    pub backup_provider_s3_access_key: String,
    pub backup_provider_s3_secret_key: String,
    pub backup_provider_gcp_bucket: String,
    pub backup_provider_gcp_path: String,
    pub backup_provider_gcp_key: String,
    pub backup_schedule_interval: BackupScheduleInterval,
    pub backup_schedule_start_hour: usize,
}

pub struct SettingsProvider {
    db: Arc<TransactionDB>,
}

impl SettingsProvider {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        SettingsProvider { db }
    }

    pub fn load(
        &self,
    ) -> Result<Settings> {
        let tx = self.db.transaction();
        let key = "config";

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "config not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn save(&self, cfg: &Settings) -> Result<()> {
        let tx = self.db.transaction();
        let key = "config";
        let data = serialize(cfg)?;
        tx.put(
            key,
            data,
        )?;

        tx.commit()?;
        Ok(())
    }
}

fn serialize(v: &Settings) -> Result<Vec<u8>> {
    let backup = pbconfig::Config{
        auth_access_token: v.auth_access_token.clone(),
        auth_refresh_token: v.auth_refresh_token.clone(),
        auth_admin_default_password: v.auth_admin_default_password.clone(),
        backup_enabled: v.backup_enabled,
        backup_encryption_enabled: v.backup_encryption_enabled,
        backup_encryption_password: v.backup_encryption_password.clone(),
        backup_compression_enabled: v.backup_compression_enabled,
        backup_provider: match v.backup_provider {
            BackupProvider::Local => pbconfig::BackupProvider::Local as i32,
            BackupProvider::S3 => pbconfig::BackupProvider::S3 as i32,
            BackupProvider::GCP => pbconfig::BackupProvider::Gcp as i32,
        },
        backup_provider_local: v.backup_provider_local_path.clone(),
        backup_provider_s3_bucket: v.backup_provider_s3_bucket.clone(),
        backup_provider_s3_path: v.backup_provider_s3_path.clone(),
        backup_provider_s3_region: v.backup_provider_s3_region.clone(),
        backup_provider_s3_access_key: v.backup_provider_s3_access_key.clone(),
        backup_provider_s3_secret_key: v.backup_provider_s3_secret_key.clone(),
        backup_provider_gcp_bucket: v.backup_provider_gcp_bucket.clone(),
        backup_provider_gcp_path: v.backup_provider_gcp_path.clone(),
        backup_provider_gcp_key: v.backup_provider_gcp_key.clone(),
        backup_schedule_interval: match v.backup_schedule_interval {
            BackupScheduleInterval::Hourly => pbconfig::BackupScheduleInterval::Hourly as i32,
            BackupScheduleInterval::Daily => pbconfig::BackupScheduleInterval::Daily as i32,
            BackupScheduleInterval::Weekly => pbconfig::BackupScheduleInterval::Weekly as i32,
            BackupScheduleInterval::Monthly => pbconfig::BackupScheduleInterval::Monthly as i32,
            BackupScheduleInterval::Yearly => pbconfig::BackupScheduleInterval::Yearly as i32,
        },
        backup_schedule_start_hour: 0,
    };

    Ok(backup.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Settings> {
    let c = pbconfig::Config::decode(data)?;
    Ok(Settings {
        auth_access_token: c.auth_access_token,
        auth_refresh_token: c.auth_refresh_token,
        auth_admin_default_password: c.auth_admin_default_password,
        backup_enabled: c.backup_enabled,
        backup_encryption_enabled: c.backup_encryption_enabled,
        backup_encryption_password: c.backup_encryption_password,
        backup_compression_enabled: c.backup_compression_enabled,
        backup_provider: match c.backup_provider {
            1 => BackupProvider::Local,
            2 => BackupProvider::S3,
            3 => BackupProvider::GCP,
            _=>panic!("Invalid backup provider")
        },
        backup_provider_local_path: c.backup_provider_local,
        backup_provider_s3_bucket: c.backup_provider_s3_bucket,
        backup_provider_s3_path: c.backup_provider_s3_path,
        backup_provider_s3_region: c.backup_provider_s3_region,
        backup_provider_s3_access_key: c.backup_provider_s3_access_key,
        backup_provider_s3_secret_key: c.backup_provider_s3_secret_key,
        backup_provider_gcp_bucket: c.backup_provider_gcp_bucket,
        backup_provider_gcp_path: c.backup_provider_gcp_path,
        backup_provider_gcp_key: c.backup_provider_gcp_key,
        backup_schedule_interval: match c.backup_schedule_interval {
            1 => BackupScheduleInterval::Hourly,
            2 => BackupScheduleInterval::Daily,
            3 => BackupScheduleInterval::Weekly,
            4 => BackupScheduleInterval::Monthly,
            5 => BackupScheduleInterval::Yearly,
            _=>panic!("Invalid backup schedule interval")
        },
        backup_schedule_start_hour: c.backup_schedule_start_hour as usize,
    })
}

#[cfg(test)]
mod tests {
    use crate::settings::{BackupProvider, BackupScheduleInterval, Settings};

    #[test]
    fn test_roundtrip() {
       let settings = Settings {
           auth_access_token: "1".to_string(),
           auth_refresh_token: "2".to_string(),
           auth_admin_default_password: "3".to_string(),
           backup_enabled: true,
           backup_encryption_enabled: true,
           backup_encryption_password: "4".to_string(),
           backup_compression_enabled: true,
           backup_provider: BackupProvider::Local,
           backup_provider_local_path: "4.1".to_string(),
           backup_provider_s3_bucket: "5".to_string(),
           backup_provider_s3_path: "6".to_string(),
           backup_provider_s3_region: "7".to_string(),
           backup_provider_s3_access_key: "8".to_string(),
           backup_provider_s3_secret_key: "9".to_string(),
           backup_provider_gcp_bucket: "10".to_string(),
           backup_provider_gcp_path: "11".to_string(),
           backup_provider_gcp_key: "12".to_string(),
           backup_schedule_interval: BackupScheduleInterval::Hourly,
           backup_schedule_start_hour: 0,
       };

        let data = super::serialize(&settings).unwrap();
        let settings2 = super::deserialize(&data).unwrap();
        assert_eq!(settings, settings2);
    }
}