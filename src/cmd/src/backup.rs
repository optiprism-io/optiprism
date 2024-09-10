use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use std::fs;
use std::time::Duration;
use chrono::{DateTime, NaiveDateTime, Timelike, Utc};
use croner::Cron;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, WriteMultipart};
use tokio::task;
use tokio::time::sleep;
use tracing::{debug, error};
use zip::AesMode;
use zip::write::SimpleFileOptions;
use common::config::Config;
use common::{DATA_PATH_BACKUP_TMP, DATA_PATH_BACKUPS};
use metadata::{backups, MetadataProvider};
use metadata::backups::{Backup, CreateBackupRequest, Provider};
use metadata::settings::{BackupProvider, BackupScheduleInterval, Settings};
use storage::db::OptiDBImpl;
use crate::error::Result;

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
            if p.exists() {
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
            sleep(Duration::from_secs(10)).await;
            let settings = md.settings.load().expect("load config error");
            // cancel if not enabled
            if !settings.backup_enabled {
                continue;
            }

            let backups = md_cloned.backups.list().expect("list backups error");
            let maybe_idle = backups.into_iter().find(|b| b.status == backups::Status::Idle);
            if let Some(backup) = maybe_idle {
                match run_backup(backup.clone(), db_cloned.clone(), md_cloned.clone(), cfg.clone(), &settings).await {
                    Ok(b) => {
                        md_cloned.backups.update_status(b.id, metadata::backups::Status::Completed).expect("update status error");
                    }
                    Err(e) => {
                        error!("backup error: {:?}", e);
                        md_cloned.backups.update_status(backup.id, metadata::backups::Status::Failed(e.to_string())).expect("update status error");
                    }
                };
                continue;
            }

            let cron = match settings.backup_schedule_interval {
                BackupScheduleInterval::Hourly => "0 * * * *".to_string(),
                BackupScheduleInterval::Daily => format!("0 {} * * *", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Weekly => format!("0 {} * * 0", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Monthly => format!("0 {} 1 * *", settings.backup_schedule_start_hour),
                BackupScheduleInterval::Yearly => format!("0 {} 1 1 *", settings.backup_schedule_start_hour),
            };

            // let cron = "* * * * *";
            let cron = Cron::new(&cron).parse().expect("cron schedule parse error");
            let cur_time = Utc::now().naive_utc();
            let cur_time = truncate_to_minute(&cur_time);
            let next_time = cron.find_next_occurrence(&DateTime::from_timestamp(cur_time.and_utc().timestamp(), 0).unwrap(), true).expect("find next occurrence error").naive_utc();
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

            let backup = create_backup(md.clone()).expect("create backup error");
            match run_backup(backup.clone(), db_cloned.clone(), md_cloned.clone(), cfg.clone(), &settings).await {
                Ok(b) => {
                    md_cloned.backups.update_status(b.id, metadata::backups::Status::Completed).expect("update status error");
                }
                Err(e) => {
                    error!("backup error: {:?}", e);
                    md_cloned.backups.update_status(backup.id, metadata::backups::Status::Failed(e.to_string())).expect("update status error");
                }
            };
        }
    });
    Ok(())
}

async fn run_backup(backup: Backup, db: Arc<OptiDBImpl>, md: Arc<MetadataProvider>, cfg: Config, settings: &Settings) -> Result<Backup> {
    let md_cloned = md.clone();
    let tmp_path = cfg.data.path.join(DATA_PATH_BACKUP_TMP).join(backup.id.to_string());
    let tmp_path_cloned = tmp_path.clone();
    let backup_cloned = backup.clone();
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

    let req = CreateBackupRequest::from_settings(&settings);
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
    // generate backup to default dir for now
    /*let dst_path = match &backup.provider {
        Provider::Local(path) => path.clone(),
        Provider::S3(_) => panic!("invalid provider"),
        Provider::GCP(_) => panic!("invalid provider")
    };*/
    dbg!(&tmp_path,&dst_path);
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

    let tmp = OpenOptions::new().read(true).open(tmp_path)?;
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
