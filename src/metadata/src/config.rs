use std::sync::{Arc, Mutex};
use std::time::Duration;
use bincode::{deserialize, serialize};
use rocksdb::TransactionDB;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::error::MetadataError;
use crate::Result;

#[derive(Clone)]
pub enum StringKey {
    AuthAccessToken,
    AuthRefreshToken,
    AuthAdminDefaultPassword,
    BackupLocalPath,
    BackupS3Bucket,
    BackupS3Region,
    BackupS3AccessKey,
    BackupS3SecretKey,
    BackupSchedule,
    BackupEncryptionKey,
}

impl StringKey {
    fn as_str(&self) -> &'static str {
        match self {
            StringKey::AuthAccessToken => "auth.access_token",
            StringKey::AuthRefreshToken => "auth.refresh_token",
            StringKey::AuthAdminDefaultPassword => "auth.admin_default_password",
            StringKey::BackupLocalPath => "backup.local_path",
            StringKey::BackupS3Bucket => "backup.s3.bucket",
            StringKey::BackupS3Region => "backup.s3.region",
            StringKey::BackupS3AccessKey => "backup.s3.access_key",
            StringKey::BackupS3SecretKey => "backup.s3.secret_key",
            StringKey::BackupSchedule => "backup.scheduler",
            StringKey::BackupEncryptionKey => "backup.encryption_key",
        }
    }
}
#[derive(Clone)]
pub enum IntKey {
    BackupProvider = 1,
}


pub enum BackupProvider {
    Local = 1,
    S3 = 2,
}

impl IntKey {
    fn as_str(&self) -> &'static str {
        match self {
            IntKey::BackupProvider => "backup.provider",
        }
    }
}
#[derive(Clone)]
pub enum BoolKey {
    BackupEnabled,
    BackupEncryptionEnabled,
}

impl crate::config::BoolKey {
    fn as_str(&self) -> &'static str {
        match self {
            BoolKey::BackupEnabled => "backup.enabled",
            BoolKey::BackupEncryptionEnabled => "backup.encryption_enabled",
        }
    }
}
#[derive(Clone)]
pub enum DecimalKey {}

impl crate::config::DecimalKey {
    fn as_str(&self) -> &'static str {
        unimplemented!()
    }
}
#[derive(Clone)]
pub enum DurationKey {}

impl crate::config::DurationKey {
    fn as_str(&self) -> &'static str {
        unimplemented!()
    }
}

pub enum Key {
    String(StringKey),
    Int(IntKey),
    Bool(BoolKey),
    Decimal(DecimalKey),
    Duration(DurationKey),
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Value {
    String(Option<String>),
    Int(Option<i64>),
    Bool(Option<bool>),
    Decimal(Option<Decimal>),
    Duration(Option<std::time::Duration>),
}

pub struct Config {
    db: Arc<TransactionDB>,
    subs: Arc<Mutex<Vec<(String, fn(key: Key, value: Value)->Result<()>)>>>,
}

impl Config {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Config { db, subs: Default::default() }
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
        let skey = key.as_str();
        let skey = format!("config/{}", skey);
        let svalue = serialize(&Value::String(value.clone()))?;
        self.db.put(&skey, svalue)?;

        // send to subscribers
        let subs = self.subs.lock().unwrap();
        for (_, cb) in subs.iter() {
            cb(Key::String(key.clone()), Value::String(value.clone()))?;
        }
        Ok(())
    }

    pub fn get_int(&self, key: IntKey) -> Result<Option<i64>> {
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

    pub fn set_int(&self, key: IntKey, value: Option<i64>) -> Result<()> {
        let skey = key.as_str();

        let skey = format!("config/{}", skey);
        let svalue = serialize(&Value::Int(value))?;
        self.db.put(&skey, svalue)?;

        let subs = self.subs.lock().unwrap();
        for (_, cb) in subs.iter() {
            cb(Key::Int(key.clone()), Value::Int(value.clone()))?;
        }
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
        let skey = key.as_str();

        let skey = format!("config/{}", skey);
        let svalue = serialize(&Value::Bool(value))?;
        self.db.put(&skey, svalue)?;

        let subs = self.subs.lock().unwrap();
        for (_, cb) in subs.iter() {
            cb(Key::Bool(key.clone()), Value::Bool(value.clone()))?;
        }

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
        let skey = key.as_str();

        let skey = format!("config/{}", skey);
        let svalue = serialize(&Value::Decimal(value))?;
        self.db.put(&skey, svalue)?;

        let subs = self.subs.lock().unwrap();
        for (_, cb) in subs.iter() {
            cb(Key::Decimal(key.clone()), Value::Decimal(value.clone()))?;
        }

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

    pub fn set_duration(&self, key: DurationKey, value: Option<Duration>) -> Result<()> {
        let skey = key.as_str();

        let skey = format!("config/{}", skey);
        let svalue = serialize(&Value::Duration(value))?;
        self.db.put(&skey, svalue)?;

        let subs = self.subs.lock().unwrap();
        for (_, cb) in subs.iter() {
            cb(Key::Duration(key.clone()), Value::Duration(value.clone()))?;
        }

        Ok(())
    }

    pub fn subscribe(&mut self, cb: fn(key: Key, value: Value) -> Result<()>) -> Result<String> {
        let uuid = Uuid::new_v4().to_string();
        self.subs.lock().unwrap().push((uuid.clone(), cb));
        Ok(uuid)
    }

    pub fn unsubscribe(&mut self, uuid: &str) -> Result<()> {
        let mut subs = self.subs.lock().unwrap();
        let idx = subs.iter().position(|(id, _)| id == uuid);
        if let Some(idx) = idx {
            subs.remove(idx);
            Ok(())
        } else {
            Err(MetadataError::NotFound(format!(
                "subscription {uuid} not found"
            )))
        }
    }
}