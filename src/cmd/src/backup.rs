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
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, WriteMultipart};
use object_store::local::LocalFileSystem;
use openssl::symm::Cipher;
use pbkdf2::pbkdf2_hmac;
use rand::rngs::StdRng;
use rand::SeedableRng;
use sha2::Sha256;
use tokio::task;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{debug, error, trace};
use metadata::{backup, backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, GCPProvider, Provider, S3Provider};
use metadata::settings::{BackupProvider, Settings};
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
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await;
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
            let res = backup(md_cloned.clone(), &db_cloned).await;
            match res {
                Ok(_) => {}
                Err(err) => {
                    error!("failed to backup: {:?}", err);
                }
            }
        }
    });
    /*thread::spawn(move || {
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
*/
    Ok(())
}

async fn backup(md: Arc<MetadataProvider>, db: &Arc<OptiDBImpl>) -> Result<()> {
    let cfg = md.config.load()?;
    let backup_cfg = cfg.backup.unwrap();
    let prov = backup_cfg.provider.clone();
    match backup_cfg.provider {
        metadata::settings::BackupProvider::Local(_) => {}
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
        metadata::settings::BackupProvider::Local(path) => Provider::Local(path),
        metadata::settings::BackupProvider::S3(s3) => Provider::S3(S3Provider { bucket: s3.bucket, region: s3.region }),
        metadata::settings::BackupProvider::GCP(gcp) => Provider::GCP(GCPProvider { bucket: gcp.bucket })
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
        backup_local(&db, &bak, &backup_cfg, progress).await?;
    } else if matches!(provider,Provider::GCP(_)) {
        backup_gcp(&db, &bak, &backup_cfg, progress).await?;
    };

    md.backups.update_status(bak.id, backups::Status::Completed)?;

    Ok(())
}

/*
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
*/

struct Bridge {
    w: object_store::WriteMultipart,
}

impl Bridge {
    pub async fn finish(mut self) -> Result<()> {
        self.w.finish().await?;
        Ok(())
    }
}

impl Write for Bridge {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.w.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

async fn backup_local<F: Fn(usize)>(db: &Arc<OptiDBImpl>, backup: &Backup, cfg: &metadata::settings::Backup, progress: F) -> Result<()> {
    debug!("starting local backup");
    let path = backup.path();
    let w = BufWriter::new(File::create(path)?);

    let obj = LocalFileSystem::new();
    let path = object_store::path::Path::from(backup.path());
    let upload = obj.put_multipart(&path).await?;
    let mut mp = WriteMultipart::new(upload);
    let mut w = Bridge { w: mp };

    db.full_backup(&mut w, |pct| {
        progress(pct);
    })?;

    w.finish().await?;

    debug!("backup successful");

    Ok(())
}


async fn backup_gcp<F: Fn(usize)>(db: &Arc<OptiDBImpl>, backup: &Backup, cfg: &metadata::settings::Backup, progress: F) -> Result<()> {
    debug!("starting gcp backup");

    let a = r#"
        {
            "type": "service_account",
            "project_id": "optiprism",
            "private_key_id": "f3a08e9fe78f5c896b5e0cc19473dcd1630f2f9e",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCx+l8z/P35Gotj\nIxQxdsl5oh56sm8c5iLi4mY2rrp10SNJ3mo2uheEthQp5CCu0OCsBLJRwrfh000Y\nEKVaOmMS2ngintNzKXpMRU945ekzKQEe4Bv6al2Id1vsiy8pdJIAaChAxhTr0609\nzoaEE/SxisZWsrCApA2t+Dr00ZF64F/1Jyeb1RHy3UynZ4HxtC/X90PwKa4W1JB6\n2wCHm+8M/yGGbFXvWGFsHcirIWrFBMWO3U4gx99AOCosvhgzPIDCD6QJtgS9U3bx\nUIiQaGlfJC+uaSXbao4Tzt0lDK/+h68C3s71j1OUBj2D9piA7c0J0+BY6xbc7Pg3\n56nJPbwRAgMBAAECggEAL0Q8FnWBCbAYBpshWMWgWlJI3/MVeUpRR4oy9SDQWkvR\noGOWN7SSXGdy0XFJkMPedzDEHtyksy/H0LVTBLRu7Wnh7+fYZkREu47IvWXp2fFw\n379LDuVCs+RnIFoSi2LvB3aiAhnZIoxT/Q8lQFyAZsphRFMuduuaynIbTjt99HDC\nCa2WgyowY3KTLcH6TvUVvbVCuRoxEjG5YyDWf22gVVNauVJihIGEsrz86iE0+SVE\nVDpsdEjevlkr9gxQqGvVFq9UteeaCZWFhkQBIQeaNpZqSfjtHcDy9L9btlkZmIry\nVTjin29cqfO7y2BmT9/W02xePLxvka3UI0bj/s+7lQKBgQDzzhFNJC/doipUkUOj\ntr+ajB62jb8imRyh1kUZ979w3cXkBCjt8MWK1ZLNTgsB0I09vIpiC/lZVL6KxfQZ\n41DaIw9B8jFuxQIRGuh8rP6HknlzCxxZ3HqeP/JNwhcCoedkK0o+XCehgev6n3mv\nHc4rI+zpmtda6Mk3QydqIe+WrwKBgQC64WM/DBKGHTSZkLe+27KqHJo58/UNLynx\ne9rJIGJm31E+YaNwmbeqRJATe9OLQ7ZboVV06wAp4tPpv9RpSla8P+QE/7NM9GW1\nYj8MW8bKoYSVxcX6a5oh1zXgp6aXQqSiFwd75lhS5xMaHoKBZQiPL2HyJuTiJlVp\ne8ndK5yJPwKBgQDOqsWbwKsakxaS7TiLFKTC2zhFw05cg7HztfCJnKuZf0T6jlQr\nrselcnmosxk9ho3T4XjkuAW8pcuHU1oif8DPyJxsaGNi5HlmCos89GAmiBGPZcG4\not8GOmqpY3eh8aB2FwQubGvjyoBAyOKbgQZ9J0zykSEwnNfEkpZcrzurXQKBgQCP\nz5xxSxgCLv1oY46TCDxQXlxs1oiwkafkVlyCRDKVWasKp1Z/8zr8g3CgHb0oQX5W\nuyupIqLomM5c5itOr09Z5IzTL/bJ9JVEZQuBtiqfinYeT6jP0fg1rIigjkNLyZQp\nzDENLrCvc3Umt23Up2xTy7HDCB1AzyERYJpyYfo/PwKBgQCUmoC78M7LCUkRcbqS\n4OkCSbc1JvwmocAr3EMRsT26J4fCgKcPRg+HV8z7N6KGRg4A4UaBw6U3j/zZ0b46\nTUf445UDWFmDiFxcq1kJhirDCfPPfy2eMLrlI29L35LzvYtqtK2/WIwZek1wvicC\nUchQxFt5LrFO2ivzFU9RQxOsTg==\n-----END PRIVATE KEY-----\n",
            "client_email": "test-baclup@optiprism.iam.gserviceaccount.com",
            "client_id": "115313246361308233448",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-baclup%40optiprism.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }"#;
    let gcs = GoogleCloudStorageBuilder::new().with_bucket_name("optiprism").with_service_account_key(a).build()?;
    let path = object_store::path::Path::from("bak");
    let upload = gcs.put_multipart(&path).await?;
    let mut w = WriteMultipart::new(upload);
    w.write(b"hello");
    w.put()
    w.finish().await.unwrap();
    db.full_backup(&mut w, |pct| {
        progress(pct);
    })?;

    debug!("backup successful");

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::Path;
    use base64::decode;
    use cryptostream::write::Encryptor;
    use datafusion::parquet::data_type::AsBytes;
    use object_store::gcp::GoogleCloudStorageBuilder;
    use object_store::{ObjectStore, WriteMultipart};
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

    #[tokio::test]
    async fn test_gcp() {
        let a = r#"
        {
            "type": "service_account",
            "project_id": "optiprism",
            "private_key_id": "f3a08e9fe78f5c896b5e0cc19473dcd1630f2f9e",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCx+l8z/P35Gotj\nIxQxdsl5oh56sm8c5iLi4mY2rrp10SNJ3mo2uheEthQp5CCu0OCsBLJRwrfh000Y\nEKVaOmMS2ngintNzKXpMRU945ekzKQEe4Bv6al2Id1vsiy8pdJIAaChAxhTr0609\nzoaEE/SxisZWsrCApA2t+Dr00ZF64F/1Jyeb1RHy3UynZ4HxtC/X90PwKa4W1JB6\n2wCHm+8M/yGGbFXvWGFsHcirIWrFBMWO3U4gx99AOCosvhgzPIDCD6QJtgS9U3bx\nUIiQaGlfJC+uaSXbao4Tzt0lDK/+h68C3s71j1OUBj2D9piA7c0J0+BY6xbc7Pg3\n56nJPbwRAgMBAAECggEAL0Q8FnWBCbAYBpshWMWgWlJI3/MVeUpRR4oy9SDQWkvR\noGOWN7SSXGdy0XFJkMPedzDEHtyksy/H0LVTBLRu7Wnh7+fYZkREu47IvWXp2fFw\n379LDuVCs+RnIFoSi2LvB3aiAhnZIoxT/Q8lQFyAZsphRFMuduuaynIbTjt99HDC\nCa2WgyowY3KTLcH6TvUVvbVCuRoxEjG5YyDWf22gVVNauVJihIGEsrz86iE0+SVE\nVDpsdEjevlkr9gxQqGvVFq9UteeaCZWFhkQBIQeaNpZqSfjtHcDy9L9btlkZmIry\nVTjin29cqfO7y2BmT9/W02xePLxvka3UI0bj/s+7lQKBgQDzzhFNJC/doipUkUOj\ntr+ajB62jb8imRyh1kUZ979w3cXkBCjt8MWK1ZLNTgsB0I09vIpiC/lZVL6KxfQZ\n41DaIw9B8jFuxQIRGuh8rP6HknlzCxxZ3HqeP/JNwhcCoedkK0o+XCehgev6n3mv\nHc4rI+zpmtda6Mk3QydqIe+WrwKBgQC64WM/DBKGHTSZkLe+27KqHJo58/UNLynx\ne9rJIGJm31E+YaNwmbeqRJATe9OLQ7ZboVV06wAp4tPpv9RpSla8P+QE/7NM9GW1\nYj8MW8bKoYSVxcX6a5oh1zXgp6aXQqSiFwd75lhS5xMaHoKBZQiPL2HyJuTiJlVp\ne8ndK5yJPwKBgQDOqsWbwKsakxaS7TiLFKTC2zhFw05cg7HztfCJnKuZf0T6jlQr\nrselcnmosxk9ho3T4XjkuAW8pcuHU1oif8DPyJxsaGNi5HlmCos89GAmiBGPZcG4\not8GOmqpY3eh8aB2FwQubGvjyoBAyOKbgQZ9J0zykSEwnNfEkpZcrzurXQKBgQCP\nz5xxSxgCLv1oY46TCDxQXlxs1oiwkafkVlyCRDKVWasKp1Z/8zr8g3CgHb0oQX5W\nuyupIqLomM5c5itOr09Z5IzTL/bJ9JVEZQuBtiqfinYeT6jP0fg1rIigjkNLyZQp\nzDENLrCvc3Umt23Up2xTy7HDCB1AzyERYJpyYfo/PwKBgQCUmoC78M7LCUkRcbqS\n4OkCSbc1JvwmocAr3EMRsT26J4fCgKcPRg+HV8z7N6KGRg4A4UaBw6U3j/zZ0b46\nTUf445UDWFmDiFxcq1kJhirDCfPPfy2eMLrlI29L35LzvYtqtK2/WIwZek1wvicC\nUchQxFt5LrFO2ivzFU9RQxOsTg==\n-----END PRIVATE KEY-----\n",
            "client_email": "test-baclup@optiprism.iam.gserviceaccount.com",
            "client_id": "115313246361308233448",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-baclup%40optiprism.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        }"#;
        let gcs = GoogleCloudStorageBuilder::new().with_bucket_name("optiprism").with_service_account_key(a).build().unwrap();
        let path = object_store::path::Path::from("bak");
        let upload = gcs.put_multipart(&path).await.unwrap();
        let mut write = WriteMultipart::new(upload);
        write.write(b"hello");
        write.finish().await.unwrap();
    }
}