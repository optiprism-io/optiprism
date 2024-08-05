use std::sync::Arc;
use std::time::Duration;
use bincode::{deserialize, serialize};
use rocksdb::TransactionDB;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::error::MetadataError;
use crate::Result;

pub enum StringKey {
    AuthAccessToken,
    AuthRefreshToken,
    AuthAdminDefaultPassword,
    BackupS3Bucket,
    BackupS3Region,
    BackupS3AccessKey,
    BackupS3SecretKey,
}

impl StringKey {
    fn as_str(&self) -> &'static str {
        match self {
            StringKey::AuthAccessToken => "auth.access_token",
            StringKey::AuthRefreshToken => "auth.refresh_token",
            StringKey::AuthAdminDefaultPassword => "auth.admin_default_password",
            StringKey::BackupS3Bucket => "backup.s3.bucket",
            StringKey::BackupS3Region => "backup.s3.region",
            StringKey::BackupS3AccessKey => "backup.s3.access_key",
            StringKey::BackupS3SecretKey => "backup.s3.secret_key",
        }
    }
}

pub enum IntKey {
    BackupFrequency,
    BackupFrequencyUnit,
    BackupStartHour,
}

pub enum BackupUnit {
    Hour = 1,
    Day = 2,
    Week = 3,
    Month = 4,
}

impl IntKey {
    fn as_str(&self) -> &'static str {
        match self {
            IntKey::BackupFrequency => "backup.frequency",
            IntKey::BackupFrequencyUnit => "backup.frequency_unit",
            IntKey::BackupStartHour => "backup.start_hour",
        }
    }
}

pub enum BoolKey {
    BackupS3Enabled,
}

impl crate::config::BoolKey {
    fn as_str(&self) -> &'static str {
        match self {
            BoolKey::BackupS3Enabled => "backup.s3.enabled",
        }
    }
}

pub enum DecimalKey {}

impl crate::config::DecimalKey {
    fn as_str(&self) -> &'static str {
        unimplemented!()
    }
}

pub enum DurationKey {}

impl crate::config::DurationKey {
    fn as_str(&self) -> &'static str {
        unimplemented!()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum Value {
    String(Option<String>),
    Int(Option<i64>),
    Bool(Option<bool>),
    Decimal(Option<Decimal>),
    Duration(Option<std::time::Duration>),
}

pub struct Config {
    db: Arc<TransactionDB>,
}

impl Config {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Config { db }
    }

    pub fn get_string(&self, key: StringKey) -> Result<Option<String>> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        match self.db.get(&key)? {
            None => Err(MetadataError::NotFound(format!(
                "key {key} not found in config"
            ))),
            Some(value) => {
                let v: Value = deserialize(&value)?;
                if let Value::String(s) = v {
                    Ok(s)
                } else {
                    Err(MetadataError::NotFound(format!(
                        "key {key} not found in config"
                    )))
                }
            }
        }
    }

    pub fn set_string(&self, key: StringKey, value: Option<String>) -> Result<()> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        let value = serialize(&Value::String(value))?;
        self.db.put(&key, value)?;
        Ok(())
    }

    pub fn get_int(&self, key: StringKey) -> Result<Option<i64>> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        match self.db.get(&key)? {
            None => Err(MetadataError::NotFound(format!(
                "key {key} not found in config"
            ))),
            Some(value) => {
                let v: Value = deserialize(&value)?;
                if let Value::Int(s) = v {
                    Ok(s)
                } else {
                    Err(MetadataError::NotFound(format!(
                        "key {key} not found in config"
                    )))
                }
            }
        }
    }

    pub fn set_int(&self, key: StringKey, value: Option<i64>) -> Result<()> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        let value = serialize(&Value::Int(value))?;
        self.db.put(&key, value)?;
        Ok(())
    }

    pub fn get_bool(&self, key: BoolKey) -> Result<Option<bool>> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        match self.db.get(&key)? {
            None => Err(MetadataError::NotFound(format!(
                "key {key} not found in config"
            ))),
            Some(value) => {
                let v: Value = deserialize(&value)?;
                if let Value::Bool(s) = v {
                    Ok(s)
                } else {
                    Err(MetadataError::NotFound(format!(
                        "key {key} not found in config"
                    )))
                }
            }
        }
    }

    pub fn set_bool(&self, key: BoolKey, value: Option<bool>) -> Result<()> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        let value = serialize(&Value::Bool(value))?;
        self.db.put(&key, value)?;
        Ok(())
    }

    pub fn get_decimal(&self, key: DecimalKey) -> Result<Option<Decimal>> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        match self.db.get(&key)? {
            None => Err(MetadataError::NotFound(format!(
                "key {key} not found in config"
            ))),
            Some(value) => {
                let v: Value = deserialize(&value)?;
                if let Value::Decimal(s) = v {
                    Ok(s)
                } else {
                    Err(MetadataError::NotFound(format!(
                        "key {key} not found in config"
                    )))
                }
            }
        }
    }

    pub fn set_decimal(&self, key: DecimalKey, value: Option<Decimal>) -> Result<()> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        let value = serialize(&Value::Decimal(value))?;
        self.db.put(&key, value)?;
        Ok(())
    }

    pub fn get_duration(&self, key: DurationKey) -> Result<Option<Duration>> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        match self.db.get(&key)? {
            None => Err(MetadataError::NotFound(format!(
                "key {key} not found in config"
            ))),
            Some(value) => {
                let v: Value = deserialize(&value)?;
                if let Value::Duration(s) = v {
                    Ok(s)
                } else {
                    Err(MetadataError::NotFound(format!(
                        "key {key} not found in config"
                    )))
                }
            }
        }
    }

    pub fn set_duration(&self, key: StringKey, value: Option<Duration>) -> Result<()> {
        let key = key.as_str();

        let key = format!("config/{}", key);
        let value = serialize(&Value::Duration(value))?;
        self.db.put(&key, value)?;
        Ok(())
    }
}