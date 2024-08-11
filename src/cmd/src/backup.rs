#![feature(async_closure)]

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use chrono::{Datelike, DateTime, NaiveDateTime, NaiveTime, Timelike, Utc};
use croner::Cron;
use cryptostream::write::Encryptor;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use openssl::symm::Cipher;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::task;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{debug, error, trace};
use common::config::Config;
use metadata::{backup, backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, Provider};
use metadata::config::{BoolKey, IntKey, Key, StringKey};
use storage::db::OptiDBImpl;
use crate::error::Error::BackupError;
use crate::error::Result;
use crate::get_random_key64;

fn truncate_to_minute(dt: &NaiveDateTime) -> NaiveDateTime {
    let dt = Some(dt);
    let dt = dt
        .and_then(|d| d.with_nanosecond(0))
        .and_then(|d| d.with_second(0));
    dt.unwrap()
}

pub async fn init(md: Arc<MetadataProvider>,
                  db: Arc<OptiDBImpl>,
                  cfg: Config) -> Result<()> {
    let backups = md.backups.list()?;
    // reset all in progress backups since they are stateless
    for backup in backups {
        if matches!(backup.status, backups::Status::InProgress(_)) {
            md.backups.update_status(backup.id, backups::Status::Failed("server restart".to_string()))?;
        };
    }

    // schedule backups
    let db_cloned = db.clone();
    let md_cloned = md.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            // cancel if not enabled
            if let Some(v) = md.config.get_bool(BoolKey::BackupEnabled).expect("get bool error") {
                if !v {
                    continue;
                }
            }

            let schedule = md.config.get_string(StringKey::BackupSchedule).expect("get string error").expect("backup scheduler not set");
            let cron = Cron::new(&schedule).parse().expect("cron schedule parse error");
            let cur_time = Utc::now().naive_utc();
            let cur_time = truncate_to_minute(&cur_time);
            let next_time = cron.find_next_occurrence(&DateTime::from_timestamp(cur_time.timestamp(),0).unwrap(), true).expect("find next occurrence error").naive_utc();
            let next_time = truncate_to_minute(&next_time);
            if cur_time != next_time {
                continue;
            }

            let res = backup(&md_cloned, &db_cloned).await;
            match res {
                Ok(_) => {}
                Err(err) => {
                    error!("failed to backup: {:?}", err);
                }
            }
        }
    });

    Ok(())
}

async fn backup(md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>) -> Result<()> {
    let prov_id = md.config.get_int(IntKey::BackupProvider)?.expect("backup provider not set");
    let provider = if prov_id as i32 == metadata::config::BackupProvider::Local as i32 {
        Provider::Local(PathBuf::from(md.config.get_string(StringKey::BackupLocalPath)?.expect("local path not set")))
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
        provider: provider.clone(),
        is_encrypted,
        iv,
    };
    let bak = md.backups.create(req)?;

    if matches!(provider,Provider::Local(_)) {
        backup_local(&md, &db, &bak)?;
    } else {
        unimplemented!();
    };

    md.backups.update_status(bak.id, backups::Status::Completed)?;

    Ok(())
}

fn backup_local(md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>, backup: &Backup) -> Result<()> {
    debug!("starting local backup");
    let path = backup.path();
    let w = BufWriter::new(File::create(path)?);
    let w: Box<dyn Write> = if backup.is_encrypted {
        let key = md.config.get_string(StringKey::BackupEncryptionKey)?.expect("encryption key not set");
        Box::new(Encryptor::new(w, Cipher::aes_128_cbc(), key.as_bytes(), backup.iv.clone().unwrap().as_bytes())?)
    } else {
        Box::new(w)
    };

    let mut w = ZlibEncoder::new(w, Compression::default());
    db.full_backup(&mut w)?;
    w.finish()?;

    debug!("backup successful");

    Ok(())
}
