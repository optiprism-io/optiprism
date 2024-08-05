use std::str::from_utf8;
use std::str::pattern::Pattern;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use prost::Message;
use rocksdb::{Transaction, TransactionDB};
use serde::{Deserialize, Serialize};
use common::types::OptionalProperty;
use crate::{account, backup, make_data_key, make_data_value_key, make_id_seq_key, make_index_key, Result};
use crate::error::MetadataError;
use crate::index::{check_insert_constraints, check_update_constraints, delete_index, get_index, insert_index, next_seq, update_index};
use crate::metadata::{ListResponse, ResponseMetadata};

const NAMESPACE: &[u8] = b"system/backups";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct S3Provider {
    bucket: String,
    region: String,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum Provider {
    Local(String),
    S3(S3Provider),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum Status {
    InProgress(usize),
    Failed(String),
    Completed,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Backup {
    pub id: u64,
    pub backup_date: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub provider: Provider,
    pub status: Status,
}

impl Backup {
    pub fn path(&self) -> String {
        match &self.provider {
            Provider::Local(path) => format!("{}/{}", path, self.backup_date.format("%Y-%m-%d %H:00:00").to_string()),
            Provider::S3(s3) => format!("s3://{}/{}", s3.bucket, self.backup_date.format("%Y-%m-%d %H:00:00").to_string()),
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
        let created_at = Utc::now();
        let id = next_seq(&tx, make_id_seq_key(NAMESPACE))?;

        let backup = Backup {
            id,
            backup_date: req.backup_date,
            created_at,
            updated_at: None,
            provider: req.provider,
            status: Status::InProgress(0),
        };

        let data = serialize(&backup)?;
        tx.put(make_data_value_key(NAMESPACE, backup.id), &data)?;
        tx.commit()?;

        Ok(backup)
    }

    pub fn get_by_id(&self, id: u64) -> Result<Backup> {
        let tx = self.db.transaction();
        self.get_by_id_(&tx, id)
    }

    pub fn list(&self) -> Result<ListResponse<Backup>> {
        let tx = self.db.transaction();
        let prefix = make_data_key(NAMESPACE);

        let iter = tx.prefix_iterator(prefix.clone());
        let mut list = vec![];
        for kv in iter {
            let (key, value) = kv?;
            // check if key contains the prefix
            if !from_utf8(&prefix).unwrap().is_prefix_of(from_utf8(&key).unwrap()) {
                break;
            }
            list.push(deserialize(&value)?);
        }

        Ok(ListResponse {
            data: list,
            meta: ResponseMetadata { next: None },
        })
    }

    pub fn update_status(&self, backup_id: u64, status: Status) -> Result<()> {
        let tx = self.db.transaction();

        let mut backup = self.get_by_id_(&tx, backup_id)?;
        backup.status = status;
        backup.updated_at = Some(Utc::now());

        let data = serialize(&backup)?;
        tx.put(make_data_value_key(NAMESPACE, backup.id), &data)?;
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
    pub backup_date: DateTime<Utc>,
    pub provider: Provider,
}

fn serialize(b: &Backup) -> Result<Vec<u8>> {
    let provider = match b.provider {
        Provider::Local(_) => backup::Provider::Local as i32,
        Provider::S3(_) => backup::Provider::S3 as i32,
    };

    let local_path = match &b.provider {
        Provider::Local(path) => Some(path.clone()),
        _ => None,
    };

    let s3_bucket = match &b.provider {
        Provider::S3(s3) => Some(s3.bucket.clone()),
        _ => None,
    };

    let s3_region = match &b.provider {
        Provider::S3(s3) => Some(s3.region.clone()),
        _ => None,
    };

    let status = match &b.status {
        Status::InProgress(p) => backup::Status::InProgress as i32,
        Status::Failed(e) => backup::Status::Failed as i32,
        Status::Completed => backup::Status::Completed as i32,
    };

    let status_failed_error = match &b.status {
        Status::Failed(e) => Some(e.clone()),
        _ => None,
    };

    let status_in_progress_progress = match &b.status {
        Status::InProgress(p) => Some(*p as i64),
        _ => None,
    };
    let b = backup::Backup {
        id: b.id,
        backup_date: b.backup_date.timestamp(),
        created_at: b.created_at.timestamp(),
        updated_at: b.updated_at.map(|t| t.timestamp()),
        provider,
        local_path,
        s3_bucket,
        s3_region,
        status,
        status_failed_error,
        status_in_progress_progress,
    };

    Ok(b.encode_to_vec())
}
fn deserialize(data: &[u8]) -> Result<Backup> {
    let from = backup::Backup::decode(data.as_ref())?;
    let provider = match from.provider {
        1 => Provider::Local(from.local_path.unwrap()),
        2 => Provider::S3(S3Provider {
            bucket: from.s3_bucket.unwrap(),
            region: from.s3_region.unwrap(),
        }),
        _ => return Err(MetadataError::Internal("invalid provider".to_string())),
    };

    let status = match from.status {
        1 => Status::InProgress(from.status_in_progress_progress.unwrap() as usize),
        2 => Status::Failed(from.status_failed_error.unwrap()),
        3 => Status::Completed,
        _ => return Err(MetadataError::Internal("invalid status".to_string())),
    };
    Ok(Backup {
        id: from.id,
        backup_date: chrono::DateTime::from_timestamp(from.backup_date, 0).unwrap(),
        created_at: chrono::DateTime::from_timestamp(from.created_at, 0).unwrap(),
        updated_at: from.updated_at.map(|t| chrono::DateTime::from_timestamp(t, 0).unwrap()),
        provider,
        status,
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    #[test]
    fn test_roundtrip() {
        let b = super::Backup {
            id: 1,
            backup_date: DateTime::from_timestamp(1,0).unwrap(),
            created_at: DateTime::from_timestamp(2,0).unwrap(),
            updated_at: Some(DateTime::from_timestamp(3,0).unwrap()),
            provider: super::Provider::Local("/tmp".to_string()),
            status: super::Status::InProgress(10),
        };
        let data = super::serialize(&b).unwrap();
        let b2 = super::deserialize(&data).unwrap();
        assert_eq!(b, b2);
    }
}