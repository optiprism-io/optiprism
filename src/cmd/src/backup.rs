#![feature(async_closure)]

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use chrono::{Datelike, DateTime, NaiveDateTime, NaiveTime, Timelike, Utc};
use croner::Cron;
use cryptostream::write::Encryptor;
use datafusion::parquet::data_type::AsBytes;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use openssl::symm::Cipher;
use pbkdf2::pbkdf2_hmac;
use rand::rngs::StdRng;
use rand::SeedableRng;
use sha2::Sha256;
use tokio::task;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{debug, error, trace};
use metadata::{backup, backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, GCPProvider, Provider, S3Provider};
use metadata::config::{BackupProvider, Config};
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
                  db: Arc<OptiDBImpl>) -> Result<()> {
    let backups = md.backups.list()?;
    // reset all in progress backups since they are stateless
    for backup in backups {
        if matches!(backup.status, backups::Status::InProgress(_)) {
            md.backups.update_status(backup.id, backups::Status::Failed("server restarted".to_string()))?;
        };
    }

    // schedule backups
    let db_cloned = db.clone();
    let md_cloned = md.clone();

    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_secs(5));
            let cfg = md.config.load().expect("load config error");
            // cancel if not enabled
            if cfg.backup.is_none() {
                continue;
            }
            let backup_cfg = cfg.backup.unwrap();

            let schedule = backup_cfg.schedule;
            let cron = Cron::new(&schedule).parse().expect("cron schedule parse error");
            let cur_time = Utc::now().naive_utc();
            let cur_time = truncate_to_minute(&cur_time);
            let next_time = cron.find_next_occurrence(&DateTime::from_timestamp(cur_time.timestamp(), 0).unwrap(), true).expect("find next occurrence error").naive_utc();
            let next_time = truncate_to_minute(&next_time);
            if cur_time != next_time {
                continue;
            }
            let backups = md.backups.list().expect("list backups error");
            if let Some(last) = backups.data.last() {
                let cur_time = last.created_at.naive_utc();
                let cur_time = truncate_to_minute(&cur_time);
                if cur_time == next_time {
                    continue;
                }
            }
            let res = backup(md_cloned.clone(), &db_cloned);
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

fn backup(md: Arc<MetadataProvider>, db: &Arc<OptiDBImpl>) -> Result<()> {
    let cfg = md.config.load()?;
    let backup_cfg = cfg.backup.unwrap();
    let prov = backup_cfg.provider.clone();
    match backup_cfg.provider {
        metadata::config::BackupProvider::Local(_) => {}
        _ => panic!("invalid backup provider: {:?}", prov)
    }
    let iv = if let Some(e) = &backup_cfg.encryption {
        let mut rng = StdRng::from_rng(rand::thread_rng())?;
        let key = get_random_key64(&mut rng);
        Some(key.to_vec())
    } else {
        None
    };

    let provider = match prov {
        metadata::config::BackupProvider::Local(path) => Provider::Local(path),
        metadata::config::BackupProvider::S3(s3) => Provider::S3(S3Provider { bucket: s3.bucket, region: s3.region }),
        metadata::config::BackupProvider::GCP(gcp) => Provider::GCP(GCPProvider { bucket: gcp.bucket })
    };

    let req = CreateBackupRequest {
        provider: provider.clone(),
        is_encrypted: backup_cfg.encryption.is_some(),
        is_compressed: backup_cfg.compression_enabled,
        iv,
    };

    let bak = md.backups.create(req)?;
    let progress = |pct: usize| {
        md.backups.update_status(bak.id, metadata::backups::Status::InProgress(pct)).expect("update status error");
    };
    if matches!(provider,Provider::Local(_)) {
        backup_local(&db, &bak, &backup_cfg, progress)?;
    } else if matches!(provider,Provider::GCP(_)) {
        unimplemented!();
    };

    md.backups.update_status(bak.id, backups::Status::Completed)?;

    Ok(())
}

fn backup_local<F: Fn(usize)>(db: &Arc<OptiDBImpl>, backup: &Backup, cfg: &metadata::config::Backup, progress: F) -> Result<()> {
    debug!("starting local backup");
    let path = backup.path();
    let w = BufWriter::new(File::create(path)?);
    let mut w: Box<dyn Write> = if let Some(enc) = &cfg.encryption {
        let pwd = enc.password.clone();
        let salt = enc.salt.clone();
        let mut key = [0u8; 16];
        pbkdf2_hmac::<Sha256>(pwd.as_slice(), salt.as_slice(), 1000, &mut key);
        Box::new(Encryptor::new(w, Cipher::aes_128_cbc(), key.as_slice(), backup.iv.clone().unwrap().as_slice())?)
    } else {
        Box::new(w)
    };

    if cfg.compression_enabled {
        let mut w = ZlibEncoder::new(w, Compression::default());
        db.full_backup(&mut w, |pct| {
            progress(pct);
        })?;
        w.finish()?;
    } else {
        db.full_backup(&mut w, |pct| {
            progress(pct);
        })?;
    }

    debug!("backup successful");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use base64::decode;
    use cryptostream::write::Encryptor;
    use datafusion::parquet::data_type::AsBytes;
    use openssl::symm::Cipher;
    use pbkdf2::pbkdf2_hmac;
    use rand::prelude::StdRng;
    use rand::SeedableRng;
    use sha2::Sha256;
    use crate::{get_random_key128, get_random_key64};

    #[test]
    fn test_encryptor() {
        let w = File::create("/tmp/zlib").unwrap();
        let password = b"password";
        let salt = b"salt";
        // number of iterations
        let n = 1000;

        let mut key1 = [0u8; 16];
        pbkdf2_hmac::<Sha256>(password, salt, n, &mut key1);
        let a = Encryptor::new(w, Cipher::aes_128_cbc(), &key1, &key1).unwrap();
    }
}