use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use cryptostream::write::Encryptor;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use openssl::symm::Cipher;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, trace};
use common::config::Config;
use metadata::{backup, backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, Provider};
use metadata::config::{BoolKey, IntKey, StringKey};
use storage::db::OptiDBImpl;
use crate::error::Error::BackupError;
use crate::error::Result;
use crate::get_random_key64;

pub async fn init(md: Arc<MetadataProvider>,
                  db: Arc<OptiDBImpl>,
                  cfg: Config) -> Result<()> {
    let backups = md.backups.list()?;
    // reset all in progress backups since they are stateless
    for backup in backups {
        if matches!(backup.status, backups::Status::InProgress) {
            md.backups.update_status(backup.id, backups::Status::Failed("server restart".to_string()))?;
        };
    }

    let mut sched = JobScheduler::new().await?;
    // schedule backups
    let db_cloned = db.clone();
    let md_cloned = md.clone();
    let s = sched.add(
        Job::new(md.config.get_string(StringKey::BackupScheduler)?.unwrap(), |_uuid, _l| {
            trace!("backup job triggered");
            let res = backup(md_cloned, db_cloned);
            match res {
                Ok(_) => {}
                Err(err) => {
                    error!("failed to backup: {:?}", err);
                }
            }
        })?
    ).await?;

    Ok(())
}

fn backup(md: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Result<()> {
    // cancel if not enabled
    if let Some(v) = md.config.get_bool(BoolKey::BackupEnabled)? {
        if !v {
            return Ok(());
        }
    }

    if let Some(backup) = md.backups.list()? {
        // cancel if backup already in progress
        if matches!(backup.status, backups::Status::InProgress) {
            return Ok(());
        }
    }

    let prov_id = md.config.get_int(IntKey::BackupProvider)?.expect("backup provider not set");
    let provider = if prov_id as i32 == metadata::config::BackupProvider::Local as i32 {
        Provider::Local(PathBuf::from(md.config.get_string(StringKey::BackupLocalPath))?.expect("local path not set"))
    } else if prov_id as i32 == metadata::config::BackupProvider::S3 as i32 {
        Provider::S3(backups::S3Provider {
            bucket: md.config.get_string(StringKey::BackupS3Bucket)?.expect("s3 bucket not set"),
            region: md.config.get_string(StringKey::BackupS3Region)?.expect("s3 region not set"),
        })
    } else {
        panic!("invalid backup provider: {}", prov_id)
    };

    let is_encrypted = md.config.get_bool(BoolKey::BackupEncryptionEnabled)?.expect("encryption not set");
    let iv = if is_encrypted {
        let mut rng = StdRng::from_rng(rand::thread_rng())?;
        let key = get_random_key64(&mut rng);
        Some(hex::encode(key))
    } else {
        None
    };

    let req = CreateBackupRequest {
        provider,
        is_encrypted,
        iv,
    };
    let bak = md.backups.create(req)?;

    if matches!(provider,Provider::Local(_)) {
        return Ok(backup_local(&md, &db, &bak)?);
    } else {
        unimplemented!();
    }
}

fn backup_local(md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>, backup: &Backup) -> Result<()> {
    trace!("local backup");
    let path = backup.path();
    let mut w = BufWriter::new(File::create(path)?);
    let w: dyn Read = if backup.is_encrypted {
        let key = md.config.get_string(StringKey::BackupEncryptionKey)?.unwrap().as_bytes();
        Encryptor::new(w, Cipher::aes_128_cbc(), key, backup.iv.clone().unwrap().as_bytes())?
    } else {
        w
    };

    let mut w = ZlibEncoder::new(w, Compression::default());
    db.full_backup(&mut w)?;
    w.finish()?;

    trace!("backup successful");

    Ok(())
}
