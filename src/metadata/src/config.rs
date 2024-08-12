use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use chrono::Utc;
use prost::Message;
use rocksdb::{Transaction, TransactionDB};
use rust_decimal::Decimal;
use uuid::Uuid;
use common::types::OptionalProperty;
use crate::error::MetadataError;
use crate::reports::{CreateReportRequest, Report, UpdateReportRequest};
use crate::{make_data_key, make_data_value_key, make_id_seq_key, pbconfig, project_ns, Result};
use crate::dashboards::Dashboard;
use crate::index::next_seq;
use crate::metadata::{ListResponse, ResponseMetadata};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Auth {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub admin_default_password: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backup {
    pub enabled: bool,
    pub encryption_enabled: bool,
    pub compression_enabled: bool,
    pub encryption_password: Option<Vec<u8>>,
    pub encryption_salt: Option<Vec<u8>>,
    pub provider: Option<BackupProvider>,
    pub local_path: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub schedule: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackupProvider {
    Local = 1,
    S3 = 2,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub auth: Auth,
    pub backup: Backup,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auth: Auth {
                access_token: None,
                refresh_token: None,
                admin_default_password: None,
            },
            backup: Backup {
                enabled: false,
                encryption_enabled: false,
                compression_enabled: false,
                encryption_password: None,
                encryption_salt: None,
                provider: None,
                local_path: None,
                s3_bucket: None,
                s3_region: None,
                s3_access_key: None,
                s3_secret_key: None,
                schedule: None,
            },
        }
    }
}
pub struct ConfigProvider {
    db: Arc<TransactionDB>,
}

impl ConfigProvider {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        ConfigProvider { db }
    }

    pub fn load(
        &self,
    ) -> Result<Config> {
        let tx = self.db.transaction();
        let key = "config";

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "config not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn save(&self, cfg: &Config) -> Result<()> {
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

fn serialize(v: &Config) -> Result<Vec<u8>> {
    let auth = pbconfig::Auth {
        access_token: v.auth.access_token.clone(),
        refresh_token: v.auth.refresh_token.clone(),
        admin_default_password: v.auth.admin_default_password.clone(),
    };

    let backup = pbconfig::Backup {
        enabled: v.backup.enabled,
        encryption_enabled: v.backup.encryption_enabled,
        compression_enabled: v.backup.compression_enabled,
        encryption_password: v.backup.encryption_password.clone(),
        encryption_salt: v.backup.encryption_salt.clone(),
        provider: match v.backup.provider {
            Some(BackupProvider::Local) => Some(pbconfig::Provider::Local as i32),
            Some(BackupProvider::S3) => Some(pbconfig::Provider::S3 as i32),
            None => None,
        },
        local_path: v.backup.local_path.clone(),
        s3_bucket: v.backup.s3_bucket.clone(),
        s3_region: v.backup.s3_region.clone(),
        s3_access_key: v.backup.s3_access_key.clone(),
        s3_secret_key: v.backup.s3_secret_key.clone(),
        schedule: v.backup.schedule.clone(),
    };
    let c = pbconfig::Config { auth: Some(auth), backup: Some(backup) };

    Ok(c.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Config> {
    let c = pbconfig::Config::decode(data)?;
    let auth = c.auth.unwrap();
    let backup = c.backup.unwrap();

    Ok(Config {
        auth: Auth {
            access_token: auth.access_token,
            refresh_token: auth.refresh_token,
            admin_default_password: auth.admin_default_password,
        },
        backup: Backup {
            enabled: backup.enabled,
            encryption_enabled: backup.encryption_enabled,
            compression_enabled: backup.compression_enabled,
            encryption_password: backup.encryption_password,
            encryption_salt: backup.encryption_salt,
            provider: match backup.provider {
                Some(1) => Some(BackupProvider::Local),
                Some(2) => Some(BackupProvider::S3),
                None => None,
                _ => return Err(MetadataError::Internal("invalid backup provider".to_string())),
            },
            local_path: backup.local_path,
            s3_bucket: backup.s3_bucket,
            s3_region: backup.s3_region,
            s3_access_key: backup.s3_access_key,
            s3_secret_key: backup.s3_secret_key,
            schedule: backup.schedule,
        },
    })
}

#[cfg(test)]
mod tests {
    use crate::config::{Auth, Backup, BackupProvider, Config};

    #[test]
    fn test_roundtrip() {
        let cfg = Config {
            auth: Auth {
                access_token: Some("1".to_string()),
                refresh_token: Some("2".to_string()),
                admin_default_password: Some("3".to_string()),
            },
            backup: Backup {
                enabled: true,
                encryption_enabled: true,
                compression_enabled: true,
                encryption_password: Some(b"password".to_vec()),
                encryption_salt: Some(b"salt".to_vec()),
                provider: Some(BackupProvider::Local),
                local_path: Some("/tmp".to_string()),
                s3_bucket: Some("bucket".to_string()),
                s3_region: Some("region".to_string()),
                s3_access_key: Some("access".to_string()),
                s3_secret_key: Some("secret".to_string()),
                schedule: Some("schedule".to_string()),
            },
        };

        let data = super::serialize(&cfg).unwrap();
        let cfg2 = super::deserialize(&data).unwrap();
        assert_eq!(cfg, cfg2);
    }
}