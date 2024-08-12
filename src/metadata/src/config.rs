use std::str::from_utf8;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use bincode::{deserialize, serialize};
use chrono::Utc;
use rocksdb::{Transaction, TransactionDB};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use common::types::OptionalProperty;
use crate::error::MetadataError;
use crate::reports::{CreateReportRequest, Report, UpdateReportRequest};
use crate::{make_data_key, make_data_value_key, make_id_seq_key, project_ns, Result};
use crate::index::next_seq;
use crate::metadata::{ListResponse, ResponseMetadata};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Auth {
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub admin_default_password: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum BackupProvider {
    Local = 1,
    S3 = 2,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Cfg {
    pub auth: Auth,
    pub backup: Backup,
}

impl Default for Cfg {
    fn default() -> Self {
        Self{ auth: Auth {
            access_token: None,
            refresh_token: None,
            admin_default_password: None,
        }, backup: Backup {
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
        } }
    }
}
pub struct Config {
    db: Arc<TransactionDB>,
}

impl Config {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Config { db }
    }

    pub fn load(
        &self,
    ) -> Result<Cfg> {
        let tx = self.db.transaction();
        let key = "config";

        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                "config not found".to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn save(&self, cfg: &Cfg) -> Result<()> {
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
