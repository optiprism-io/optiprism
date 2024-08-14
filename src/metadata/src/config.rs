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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Auth {
    pub access_token: String,
    pub refresh_token: String,
    pub admin_default_password: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupEncryption {
    pub password: Vec<u8>,
    pub salt: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct S3Provider {
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GCPProvider {
    pub bucket: String,
    pub key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BackupProvider {
    Local(PathBuf),
    S3(S3Provider),
    GCP(GCPProvider),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backup {
    pub encryption: Option<BackupEncryption>,
    pub compression_enabled: bool,
    pub provider: BackupProvider,
    pub schedule: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub auth: Auth,
    pub backup: Option<Backup>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            auth: Auth {
                access_token: "".to_string(),
                refresh_token: "".to_string(),
                admin_default_password: "".to_string(),
            },
            backup: None,
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

    let backup = if let Some(backup) = &v.backup {
        Some(pbconfig::Backup {
            compression_enabled: backup.compression_enabled,
            encryption: if let Some(encryption) = &backup.encryption {
                Some(pbconfig::BackupEncryption {
                    password: encryption.password.clone(),
                    salt: encryption.salt.clone(),
                })
            } else {
                None
            },
            provider: {
                match &backup.provider {
                    BackupProvider::Local(path) => {
                        Some(pbconfig::BackupProvider {
                            provider: pbconfig::Provider::Local as i32,
                            local_path: Some(path.to_str().unwrap().to_string()),
                            s3_bucket: None,
                            s3_region: None,
                            s3_access_key: None,
                            s3_secret_key: None,
                            gcp_bucket: None,
                            gcp_key: None,
                        })
                    }
                    BackupProvider::S3(s3) => {
                        Some(pbconfig::BackupProvider {
                            provider: pbconfig::Provider::S3 as i32,
                            local_path: None,
                            s3_bucket: Some(s3.bucket.clone()),
                            s3_region: Some(s3.region.clone()),
                            s3_access_key: Some(s3.access_key.clone()),
                            s3_secret_key: Some(s3.secret_key.clone()),
                            gcp_bucket: None,
                            gcp_key: None,
                        })
                    }
                    BackupProvider::GCP(gcp) => {
                        Some(pbconfig::BackupProvider {
                            provider: pbconfig::Provider::Gcp as i32,
                            local_path: None,
                            s3_bucket: None,
                            s3_region: None,
                            s3_access_key: None,
                            s3_secret_key: None,
                            gcp_bucket: Some(gcp.bucket.clone()),
                            gcp_key: Some(gcp.key.clone()),
                        })
                    }
                }
            },
            schedule: backup.schedule.clone(),
        })
    } else {
        None
    };

    let backup = pbconfig::Config { auth: Some(auth), backup };

    Ok(backup.encode_to_vec())
}

fn deserialize(data: &[u8]) -> Result<Config> {
    let c = pbconfig::Config::decode(data)?;
    let auth = c.auth.unwrap();
    let backup = c.backup.unwrap();

    let auth = Auth {
        access_token: auth.access_token,
        refresh_token: auth.refresh_token,
        admin_default_password: auth.admin_default_password,
    };

    let backup = Backup {
        encryption: if let Some(encryption) = backup.encryption {
            Some(BackupEncryption {
                password: encryption.password,
                salt: encryption.salt,
            })
        } else {
            None
        },
        compression_enabled: backup.compression_enabled,
        provider: backup.provider.map(|provider| {
            match provider.provider {
                1 => {
                    BackupProvider::Local(PathBuf::from(provider.local_path.unwrap()))
                }
                2 => {
                    BackupProvider::S3(S3Provider {
                        bucket: provider.s3_bucket.unwrap(),
                        region: provider.s3_region.unwrap(),
                        access_key: provider.s3_access_key.unwrap(),
                        secret_key: provider.s3_secret_key.unwrap(),
                    })
                }
                3 => {
                    BackupProvider::GCP(GCPProvider {
                        bucket: provider.gcp_bucket.unwrap(),
                        key: provider.gcp_key.unwrap(),
                    })
                }
                _ => panic!("unknown backup provider")
            }
        }).unwrap(),
        schedule: backup.schedule,
    };

    Ok(Config { auth, backup: Some(backup) })
}

#[cfg(test)]
mod tests {
    use crate::config::{Auth, Backup, BackupEncryption, BackupProvider, Config};

    #[test]
    fn test_roundtrip() {
        let cfg = Config {
            auth: Auth {
                access_token: "1".to_string(),
                refresh_token: "2".to_string(),
                admin_default_password: "3".to_string(),
            },
            backup: Some(Backup {
                encryption: Some(BackupEncryption {
                    password: vec![1, 2, 3],
                    salt: vec![4, 5, 6],
                }),
                compression_enabled: true,
                provider: BackupProvider::Local("/tmp".into()),
                schedule: "0 0 * * *".to_string(),
            }),
        };

        let data = super::serialize(&cfg).unwrap();
        let cfg2 = super::deserialize(&data).unwrap();
        assert_eq!(cfg, cfg2);
    }
}