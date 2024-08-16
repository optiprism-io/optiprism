#![feature(async_closure)]

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{fs, thread};
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
use zip::AesMode;
use zip::write::SimpleFileOptions;
use common::config::Config;
use common::{DATA_PATH_BACKUP_TMP, DATA_PATH_BACKUPS};
use metadata::{backup, backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, GCPProvider, Provider, S3Provider};
use metadata::settings::{BackupProvider, BackupScheduleInterval, Settings};
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
                  db: Arc<OptiDBImpl>, cfg: Config) -> Result<()> {
    let backups = md.backups.list()?;
    // reset all in progress backups since they are stateless
    for backup in backups {
        if matches!(backup.status, backups::Status::InProgress(_)) {
            let p = cfg.data.path.join(DATA_PATH_BACKUP_TMP).join(backup.id.to_string());
            if fs::try_exists(&p).unwrap() {
                fs::remove_file(p)?;
            }
            md.backups.update_status(backup.id, backups::Status::Failed("server restarted".to_string()))?;
        };
    }

    // schedule backups
    let db_cloned = db.clone();
    let md_cloned = md.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(5)).await;
            let settings = md.settings.load().expect("load config error");
            // cancel if not enabled
            if !settings.backup_enabled {
                continue;
            }
            let cron = match settings.backup_schedule_interval {
                BackupScheduleInterval::Hourly => "0 * * * *".to_string(),
                BackupScheduleInterval::Daily => format!("0 {} * * *", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Weekly => format!("0 {} * * 0", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Monthly => format!("0 {} 1 * *", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Yearly => format!("0 {} 1 1 *", settings.backup_schedule_start_hour),
            };

            let cron = "* * * * *";
            let cron = Cron::new(&cron).parse().expect("cron schedule parse error");
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

            let backup = match run_backup(db_cloned.clone(), md_cloned.clone(), cfg.clone(), &settings).await {
                Ok(b) => b,
                Err(e) => {
                    error!("backup error: {:?}", e);
                    continue;
                }
            };
            md_cloned.backups.update_status(backup.id, metadata::backups::Status::Completed).expect("update status error");
        }
    });
    Ok(())
}

async fn run_backup(db: Arc<OptiDBImpl>, md: Arc<MetadataProvider>, cfg: Config, settings: &Settings) -> Result<Backup> {
    let backup = create_backup(md.clone())?;
    let backup_cloned = backup.clone();
    let md_cloned = md.clone();
    let tmp_path = cfg.data.path.join(DATA_PATH_BACKUP_TMP).join(backup.id.to_string());
    let tmp_path_cloned = tmp_path.clone();
    let hnd = task::spawn_blocking(move || {
        let progress = |pct: usize| {
            md_cloned.backups.update_status(backup_cloned.id, metadata::backups::Status::InProgress(pct)).expect("update status error");
        };

        match backup_to_tmp(&db, &backup_cloned, tmp_path_cloned, progress) {
            Ok(_) => {}
            Err(e) => {
                md_cloned.backups.update_status(backup_cloned.id, metadata::backups::Status::Failed(e.to_string())).expect("update status error");
            }
        }
    });
    hnd.await.unwrap();
    if matches!(backup.provider,Provider::Local(_)) {
        backup_local(&backup, &cfg, &tmp_path)?;
    } else if matches!(backup.provider,Provider::GCP(_)) {
        md.backups.update_status(backup.id, backups::Status::Uploading)?;
        backup_gcp(&backup, settings, &tmp_path).await?;
    };
    debug!("backup successful");
    Ok(backup)
}
fn create_backup(md: Arc<MetadataProvider>) -> Result<Backup> {
    let settings = md.settings.load()?;
    match settings.backup_provider {
        BackupProvider::Local => {}
        BackupProvider::GCP => {}
        _ => panic!("invalid backup provider")
    }
    let iv = if settings.backup_encryption_enabled {
        let mut rng = StdRng::from_rng(rand::thread_rng())?;
        let key = get_random_key64(&mut rng);
        Some(key.to_vec())
    } else {
        None
    };

    let provider = match settings.backup_provider {
        BackupProvider::Local => Provider::Local(PathBuf::from(settings.backup_provider_local_path.clone())),
        BackupProvider::S3 => Provider::S3(S3Provider {
            bucket: settings.backup_provider_s3_bucket.clone(),
            path: settings.backup_provider_s3_path.clone(),
            region: settings.backup_provider_s3_region.clone(),
        }),
        BackupProvider::GCP => Provider::GCP(GCPProvider {
            bucket: settings.backup_provider_gcp_bucket.clone(),
            path: settings.backup_provider_gcp_path.clone(),
        })
    };

    let req = CreateBackupRequest {
        provider: provider.clone(),
        password: if settings.backup_encryption_enabled { Some(settings.backup_encryption_password.clone()) } else { None },
        is_encrypted: settings.backup_encryption_enabled,
    };

    Ok(md.backups.create(req)?)
}

// backup to temporary directory. Unfortunately we can't stream zip because it has Write+Seek trait
fn backup_to_tmp<F: Fn(usize)>(db: &Arc<OptiDBImpl>, backup: &Backup, tmp_path: PathBuf, progress: F) -> Result<()> {
    let tmp = File::create(tmp_path.clone())?;
    let mut zip = zip::ZipWriter::new(tmp);

    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .large_file(true)
        .unix_permissions(0o755);

    let options = if backup.is_encrypted {
        options.with_aes_encryption(AesMode::Aes128, backup.password.as_ref().expect("password not set"))
    } else {
        options
    };

    zip.start_file("backup", options)?;

    db.full_backup(&mut zip, |pct| {
        progress(pct);
    })?;

    zip.finish()?;

    Ok(())
}

fn backup_local(backup: &Backup, cfg: &Config, tmp_path: &PathBuf) -> Result<()> {
    let dst_path = cfg.data.path.join(DATA_PATH_BACKUPS).join(backup.created_at.format("%Y-%m-%dT%H:00:00.zip").to_string());
    debug!("starting local backup to {:?}",&dst_path);
    let dst_path = match &backup.provider {
        Provider::Local(path) => path.clone(),
        Provider::S3(_) => panic!("invalid provider"),
        Provider::GCP(_) => panic!("invalid provider")
    };

    fs::rename(tmp_path, &dst_path)?;

    Ok(())
}

async fn backup_gcp(backup: &Backup, settings: &Settings, tmp_path: &PathBuf) -> Result<()> {
    let gcs = GoogleCloudStorageBuilder::new()
        .with_bucket_name(settings.backup_provider_gcp_bucket.clone())
        .with_service_account_key(settings.backup_provider_gcp_key.clone())
        .build()?;
    let path = object_store::path::Path::from(backup.path());
    let upload = gcs.put_multipart(&path).await?;
    let mut w = WriteMultipart::new(upload);

    let tmp = OpenOptions::new().read(true).open(&tmp_path)?;
    let mut r = BufReader::new(tmp);
    loop {
        let mut buf = [0u8; 1024];
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        w.write(&buf[..n]);
    }
    w.finish().await.unwrap();
    fs::remove_file(tmp_path)?;

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