use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::rbac::Permission;
use metadata::backups::CreateBackupRequest;
use serde::Deserialize;
use serde::Serialize;
use metadata::{backups, MetadataProvider};
use crate::{Context, PlatformError};
use crate::ListResponse;
use crate::Result;

pub struct Backups {
    prov: Arc<MetadataProvider>,
}

impl Backups {
    pub fn new(prov: Arc<MetadataProvider>) -> Self {
        Self { prov }
    }

    pub async fn backup(
        &self,
        ctx: Context,
    ) -> Result<Backup> {
        ctx.check_permission(
            Permission::ManageServer,
        )?;

        let settings = self.prov.settings.load()?;
        if !settings.backup_enabled {
            return Err(PlatformError::Forbidden("backup is disabled".to_string()));
        }

        let req = CreateBackupRequest::from_settings(&settings);
        let backup = self.prov.backups.create(req)?;
        Ok(backup.into())
    }

    pub async fn get_by_id(&self, ctx: Context, id: u64) -> Result<Backup> {
        ctx.check_permission(
            Permission::ManageServer,
        )?;

        Ok(self.prov.backups.get_by_id(id)?.into())
    }

    pub async fn list(&self, ctx: Context) -> Result<ListResponse<Backup>> {
        ctx.check_permission(
            Permission::ManageServer,
        )?;

        let resp = self.prov.backups.list()?;
        Ok(ListResponse {
            data: resp.data.into_iter().map(|v| v.into()).collect(),
            meta: resp.meta.into(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LocalProvider {
    pub path: PathBuf,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3Provider {
    pub bucket: String,
    pub path: String,
    pub region: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GCPProvider {
    pub bucket: String,
    pub path: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Provider {
    Local(LocalProvider),
    S3(S3Provider),
    Gcp(GCPProvider),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Idle,
    InProgress,
    Uploading,
    Failed,
    Completed,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Backup {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub provider: Provider,
    pub status: Status,
    pub in_progress_progress: Option<usize>,
    pub failed_error: Option<String>,
    pub is_encrypted: bool,
}

// from metadata backup to backup
impl From<metadata::backups::Backup> for Backup {
    fn from(b: metadata::backups::Backup) -> Self {
        Backup {
            id: b.id,
            created_at: b.created_at,
            updated_at: b.updated_at,
            provider: match b.provider {
                backups::Provider::Local(path) => Provider::Local(LocalProvider { path }),
                backups::Provider::S3(prov) => Provider::S3(S3Provider {
                    bucket: prov.bucket,
                    path: prov.path,
                    region: prov.region,
                }),
                backups::Provider::GCP(prov) => Provider::Gcp(GCPProvider {
                    bucket: prov.bucket,
                    path: prov.path,
                })
            },
            status: match b.status.clone() {
                backups::Status::Idle => Status::Idle,
                backups::Status::InProgress(_) => Status::InProgress,
                backups::Status::Uploading => Status::Uploading,
                backups::Status::Failed(_) => Status::Failed,
                backups::Status::Completed => Status::Completed,
            },
            in_progress_progress: match b.status {
                backups::Status::InProgress(p) => Some(p),
                _ => None,
            },
            failed_error: match b.status {
                backups::Status::Failed(e) => Some(e),
                _ => None,
            },
            is_encrypted: b.is_encrypted,
        }
    }
}