use std::path::PathBuf;
use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use prost::Message;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;

use crate::backup;
use crate::error::MetadataError;
use crate::index::next_seq;
use crate::make_data_key;
use crate::make_data_value_key;
use crate::make_id_seq_key;
use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::settings::BackupProvider;
use crate::settings::Settings;
use crate::Result;

const NAMESPACE: &[u8] = b"system/backups";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct S3Provider {
    pub bucket: String,
    pub path: String,
    pub region: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GCPProvider {
    pub bucket: String,
    pub path: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Provider {
    Local(PathBuf),
    S3(S3Provider),
    GCP(GCPProvider),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Idle,
    InProgress(usize),
    Uploading,
    Failed(String),
    Completed,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Backup {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub provider: Provider,
    pub status: Status,
    pub is_encrypted: bool,
    pub password: Option<String>,
}

impl Backup {
    pub fn path(&self) -> String {
        match &self.provider {
            Provider::Local(path) => path
                .join(self.created_at.format("%Y-%m-%dT%H:00:00.zip").to_string())
                .into_os_string()
                .into_string()
                .unwrap(),
            Provider::S3(s3) => {
                let p = PathBuf::from(&s3.path);
                p.join(self.created_at.format("%Y-%m-%dT%H:00:00.zip").to_string())
                    .into_os_string()
                    .into_string()
                    .unwrap()
            }
            Provider::GCP(gcp) => {
                let p = PathBuf::from(&gcp.path);
                p.join(self.created_at.format("%Y-%m-%dT%H:00:00.zip").to_string())
                    .into_os_string()
                    .into_string()
                    .unwrap()
            }
        }
    }
}
pub struct Backups {
    db: Arc<TransactionDB>,
}

impl Backups {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Backups { db }
    }

    fn get_by_id_(&self, tx: &Transaction<TransactionDB>, id: u64) -> Result<Backup> {
        let key = make_data_value_key(NAMESPACE, id);
        match tx.get(key)? {
            None => Err(MetadataError::NotFound(
                format!("backup {id} not found").to_string(),
            )),
            Some(value) => Ok(deserialize(&value)?),
        }
    }

    pub fn create(&self, req: CreateBackupRequest) -> Result<Backup> {
        let tx = self.db.transaction();
        if req.status == Status::Idle {
            let list = self.list_(&tx)?;
            for backup in list.data {
                if backup.status == Status::Idle {
                    return Err(MetadataError::AlreadyExists(
                        "There is already an idle backup".to_string(),
                    ));
                }
            }
        }
        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let backup = Backup {
            id,
            created_at,
            updated_at: None,
            provider: req.provider,
            status: req.status,
            is_encrypted: req.is_encrypted,
            password: req.password,
        };

        let data = serialize(&backup)?;
        tx.put(make_data_value_key(NAMESPACE, backup.id), data)?;
        tx.commit()?;

        Ok(backup)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Backup> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, id)
    }

    pub fn list_(&self, tx: &Transaction<TransactionDB>) -> Result<ListResponse<Backup>> {
        let prefix = make_data_key(NAMESPACE);

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix)
                .unwrap()
                .is_prefix_of(from_utf8(&key).unwrap())
            {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn list(&self) -> Result<ListResponse<Backup>> {
        let tx = self.db.transaction();
        Ok(self.list_(&tx).unwrap())
    }

    pub fn update_status(&self, backup_id: u64, status: Status) -> Result<()> {
        let tx = self.db.transaction();

        let mut backup = self.get_by_id_(&tx, backup_id)?;
        backup.status = status;
        backup.updated_at = Some(Utc::now());

        let data = serialize(&backup)?;
        tx.put(make_data_value_key(NAMESPACE, backup.id), data)?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, id: u64) -> Result<()> {
        let tx = self.db.transaction();
        let _ = self.get_by_id_(&tx, id)?;
        tx.delete(make_data_value_key(NAMESPACE, id))?;

        tx.commit()?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateBackupRequest {
    pub provider: Provider,
    pub password: Option<String>,
    pub is_encrypted: bool,
    pub status: Status,
}

impl CreateBackupRequest {
    pub fn from_settings(settings: &Settings) -> Self {
        let provider = match settings.backup_provider {
            BackupProvider::Local => {
                Provider::Local(PathBuf::from(settings.backup_provider_local_path.clone()))
            }
            BackupProvider::S3 => Provider::S3(S3Provider {
                bucket: settings.backup_provider_s3_bucket.clone(),
                path: settings.backup_provider_s3_path.clone(),
                region: settings.backup_provider_s3_region.clone(),
            }),
            BackupProvider::GCP => Provider::GCP(GCPProvider {
                bucket: settings.backup_provider_gcp_bucket.clone(),
                path: settings.backup_provider_gcp_path.clone(),
            }),
        };

        CreateBackupRequest {
            provider: provider.clone(),
            password: if settings.backup_encryption_enabled {
                Some(settings.backup_encryption_password.clone())
            } else {
                None
            },
            is_encrypted: settings.backup_encryption_enabled,
            status: Status::Idle,
        }
    }
}
fn serialize(b: &Backup) -> Result<Vec<u8>> {
    let provider = match b.provider {
        Provider::Local(_) => backup::Provider::Local as i32,
        Provider::S3(_) => backup::Provider::S3 as i32,
        Provider::GCP(_) => backup::Provider::Gcp as i32,
    };

    let local_path = match &b.provider {
        Provider::Local(path) => path.clone(),
        _ => PathBuf::new(),
    };

    let s3_bucket = match &b.provider {
        Provider::S3(s3) => s3.bucket.clone(),
        _ => String::new(),
    };

    let s3_region = match &b.provider {
        Provider::S3(s3) => s3.region.clone(),
        _ => String::new(),
    };

    let gcp_bucket = match &b.provider {
        Provider::GCP(gcp) => gcp.bucket.clone(),
        _ => String::new(),
    };

    let gcp_path = match &b.provider {
        Provider::GCP(gcp) => gcp.path.clone(),
        _ => String::new(),
    };

    let s3_path = match &b.provider {
        Provider::S3(s3) => s3.path.clone(),
        _ => String::new(),
    };
    let status = match &b.status {
        Status::Idle => backup::Status::Idle as i32,
        Status::InProgress(_) => backup::Status::InProgress as i32,
        Status::Uploading => backup::Status::Uploading as i32,
        Status::Failed(_) => backup::Status::Failed as i32,
        Status::Completed => backup::Status::Completed as i32,
    };

    let status_failed_error = match &b.status {
        Status::Failed(e) => e.clone(),
        _ => String::new(),
    };

    let status_in_progress_progress = match &b.status {
        Status::InProgress(p) => *p as i64,
        _ => 0,
    };
    let b = backup::Backup {
        id: b.id,
        created_at: b.created_at.timestamp(),
        updated_at: b.updated_at.map(|t| t.timestamp()),
        provider,
        local_path: local_path.into_os_string().into_string().unwrap(),
        s3_bucket,
        s3_region,
        gcp_bucket,
        status,
        status_failed_error,
        status_in_progress_progress,
        is_encrypted: b.is_encrypted,
        s3_path,
        gcp_path,
        password: b.password.clone(),
    };

    Ok(b.encode_to_vec())
}
fn deserialize(data: &[u8]) -> Result<Backup> {
    let from = backup::Backup::decode(data)?;
    let provider = match from.provider {
        1 => Provider::Local(PathBuf::from(from.local_path)),
        2 => Provider::S3(S3Provider {
            bucket: from.s3_bucket,
            path: from.s3_path,
            region: from.s3_region,
        }),
        3 => Provider::GCP(GCPProvider {
            bucket: from.gcp_bucket,
            path: from.gcp_path,
        }),
        _ => return Err(MetadataError::Internal("invalid provider".to_string())),
    };

    let status = match from.status {
        1 => Status::Idle,
        2 => Status::InProgress(from.status_in_progress_progress as usize),
        3 => Status::Uploading,
        4 => Status::Failed(from.status_failed_error),
        5 => Status::Completed,
        _ => return Err(MetadataError::Internal("invalid status".to_string())),
    };
    Ok(Backup {
        id: from.id,
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        updated_at: from
            .updated_at
            .map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        provider,
        status,
        is_encrypted: from.is_encrypted,
        password: from.password,
    })
}

#[cfg(test)]
mod tests {

    use chrono::DateTime;

    use crate::backups::S3Provider;

    #[test]
    fn test_roundtrip() {
        let b = super::Backup {
            id: 1,
            created_at: DateTime::from_timestamp(2, 0).unwrap(),
            updated_at: Some(DateTime::from_timestamp(3, 0).unwrap()),
            provider: super::Provider::S3(S3Provider {
                bucket: "1".to_string(),
                path: "2".to_string(),
                region: "3".to_string(),
            }),
            status: super::Status::InProgress(10),
            is_encrypted: true,
            password: Some("pass".to_string()),
        };
        let data = super::serialize(&b).unwrap();
        let b2 = super::deserialize(&data).unwrap();
        assert_eq!(b, b2);
    }
}
