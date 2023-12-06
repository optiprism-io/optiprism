use std::collections::BinaryHeap;
use std::env::temp_dir;
use std::ffi::OsStr;
use std::fmt::Write;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::array::StringBuilder;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytesize::ByteSize;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use clap::Subcommand;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use dateparser::DateTimeUtc;
use demo::error::DemoError;
use demo::shop;
use demo::shop::Config;
use demo::test;
use futures::executor::block_on;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use scan_dir::ScanDir;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use platform::auth;
use platform::auth::password::make_password_hash;
use query::ProviderImpl;
use service::tracing::TracingCliArgs;
use tracing::debug;
use tracing::info;
use uuid::Uuid;
use query::datasources::local::LocalTable;
use store::db::{OptiDBImpl, Options};

extern crate parse_duration;

#[derive(Parser, Clone)]
struct Shop {
    #[arg(long)]
    demo_data_path: PathBuf,
    #[arg(long, default_value = "365 days")]
    duration: Option<String>,
    #[arg(long)]
    to_date: Option<String>,
    #[arg(long, default_value = "10")]
    new_daily_users: usize,
}

#[derive(Parser, Clone)]
struct Test {}

#[derive(Subcommand, Clone)]
enum Commands {
    /// Adds files to myapp
    Shop(Shop),
    Test(Test),
}

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    #[arg(long, global = true)]
    path: Option<PathBuf>,
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long, global = true)]
    out_parquet: Option<PathBuf>,
    #[arg(long, global = true)]
    partitions: Option<usize>,
    #[arg(long, global = true, default_value = "4096")]
    batch_size: usize,
    #[arg(long)]
    ui_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Cli::parse();
    args.tracing.init()?;

    if args.command.is_none() {
        return Err(DemoError::BadRequest("no command specified".to_string()).into());
    }


    if args.path.is_none() {
        return Err(DemoError::BadRequest("no path specified".to_string()).into());
    }
    let path = args.path.clone().unwrap();
    debug!("db path: {:?}", path);

    let rocks = Arc::new(metadata::rocksdb::new(path.clone().join("md"))?);
    let md = Arc::new(MetadataProvider::try_new(rocks)?);
    let db = Arc::new(OptiDBImpl::open(path.clone().join("store"), Options {})?);

    if let Some(ui_path) = &args.ui_path {
        if !ui_path.try_exists()? {
            return Err(
                DemoError::FileNotFound(format!("ui path {ui_path:?} doesn't exist")).into(),
            );
        }
        debug!("ui path: {:?}", ui_path);
    }

    info!("creating org structure and admin account...");
    {
        let admin = match md.accounts.create(CreateAccountRequest {
            created_by: None,
            password_hash: make_password_hash("admin")?,
            email: "admin@email.com".to_string(),
            first_name: Some("admin".to_string()),
            last_name: None,
            role: Some(Role::Admin),
            organizations: None,
            projects: None,
            teams: None,
        }) {
            Ok(acc) => acc,
            Err(err) => md.accounts.get_by_email("admin@email.com")?,
        };
        let org = match md.organizations.create(CreateOrganizationRequest {
            created_by: admin.id,
            name: "Test Organization".to_string(),
        }) {
            Ok(org) => org,
            Err(err) => md.organizations.get_by_id(1)?,
        };

        let proj1 = match md.projects.create(org.id, CreateProjectRequest {
            created_by: admin.id,
            name: "Test Project".to_string(),
        }) {
            Ok(proj) => proj,
            Err(err) => md.projects.get_by_id(1, 1)?,
        };

        let _user = match md.accounts.create(CreateAccountRequest {
            created_by: Some(admin.id),
            password_hash: make_password_hash("test")?,
            email: "user@test.com".to_string(),
            first_name: Some("user".to_string()),
            last_name: None,
            role: None,
            organizations: Some(vec![(org.id, OrganizationRole::Member)]),
            projects: Some(vec![(proj1.id, ProjectRole::Reader)]),
            teams: None,
        }) {
            Ok(acc) => acc,
            Err(err) => md.accounts.get_by_email("user@test.com")?,
        };
    }

    let data_provider: Arc<dyn TableProvider> = match &args.command {
        Some(cmd) => match cmd {
            Commands::Shop(shop) => {
                let partitions = gen_store(&args, shop, &md, &db)?;
                info!("successfully generated!");
                let mut data_size_bytes: usize = 0;
                for partition in partitions.iter() {
                    for batch in partition.iter() {
                        for column in batch.columns() {
                            data_size_bytes += column.get_array_memory_size();
                        }
                    }
                }
                debug!(
            "partitions: {}, batches: {}",
        partitions.len(),
        partitions[0].len()
    );
                debug!(
        "uncompressed dataset in-memory size: {}",
        ByteSize::b(data_size_bytes as u64)
    );
                Arc::new(MemTable::try_new(partitions[0][0].schema(), partitions)?)
            }
            Commands::Test { .. } => {
                gen_test(&args, &md, &db)?;
                info!("successfully generated!");
                Arc::new(LocalTable::try_new(db.clone(), "events".to_string())?)
            }
        },
        _ => unreachable!(),
    };


    if let Some(path) = args.out_parquet {
        db.flush()?;
        let all_parquet_files: Vec<_> = ScanDir::files().walk(args.path.unwrap().join("store/tables/events"), |iter| {
            iter.filter(|&(_, ref name)| name.ends_with(".parquet"))
                .map(|(ref entry, _)| entry.path())
                .collect()
        }).unwrap();

        for ppath in all_parquet_files {
            fs::copy(ppath.clone(), path.join(ppath.file_name().unwrap()))?;
        }
    }

    let query_provider = Arc::new(ProviderImpl::try_new_from_provider(
        md.clone(),
        data_provider,
    )?);

    let auth_cfg = auth::Config {
        access_token_duration: Duration::days(1),
        access_token_key: "access".to_owned(),
        refresh_token_duration: Duration::days(1),
        refresh_token_key: "refresh".to_owned(),
    };

    let platform_provider = Arc::new(platform::PlatformProvider::new(
        md.clone(),
        query_provider,
        auth_cfg.clone(),
    ));

    let svc = platform::http::Service::new(&md, &platform_provider, auth_cfg, args.host)
        .set_ui(args.ui_path.clone());

    info!("start listening on {}", args.host);
    if args.ui_path.is_some() {
        info!("http UI http://{}", args.host);
    }

    Ok(svc.serve().await?)
}

fn gen_test(args: &Cli, md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>) -> anyhow::Result<()> {
    info!("starting sample data generation...");
    let partitions = args.partitions.unwrap_or_else(num_cpus::get);
    test::init(partitions, md, db, 1, 1)?;
    test::gen(db, 1)?;
    Ok(())
}

fn gen_store(
    args: &Cli,
    cmd_args: &Shop,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
    let to_date = match &cmd_args.to_date {
        None => Utc::now(),
        Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
    };

    let duration = Duration::from_std(parse_duration::parse(
        cmd_args.duration.clone().unwrap().as_str(),
    )?)?;
    let from_date = to_date - duration;

    if !cmd_args.demo_data_path.try_exists()? {
        return Err(DemoError::FileNotFound(format!(
            "demo data path {:?} doesn't exist",
            cmd_args.demo_data_path
        ))
            .into());
    }
    info!("store initialization...");
    debug!("demo data path: {:?}", cmd_args.demo_data_path);
    debug!("from date {}", from_date);
    let date_diff = to_date - from_date;
    debug!("to date {}", to_date);
    debug!(
        "time range: {}",
        humantime::format_duration(date_diff.to_std()?)
    );
    debug!("new daily users: {}", cmd_args.new_daily_users);
    let total_users = cmd_args.new_daily_users as i64 * date_diff.num_days();
    info!("expecting total unique users: {total_users}");
    info!("starting sample data generation...");

    let store_cfg = Config {
        org_id: 1,
        project_id: 1,
        from_date,
        to_date,
        products_rdr: File::open(cmd_args.demo_data_path.join("products.csv"))
            .map_err(|err| DemoError::Internal(format!("can't open products.csv: {err}")))?,
        geo_rdr: File::open(cmd_args.demo_data_path.join("geo.csv"))
            .map_err(|err| DemoError::Internal(format!("can't open geo.csv: {err}")))?,
        device_rdr: File::open(cmd_args.demo_data_path.join("device.csv"))
            .map_err(|err| DemoError::Internal(format!("can't open device.csv: {err}")))?,
        new_daily_users: cmd_args.new_daily_users,
        batch_size: 4096,
        partitions: args.partitions.unwrap_or_else(num_cpus::get),
    };

    let result = shop::gen(md, db, store_cfg)?;
    let mut rows: usize = 0;
    for partition in result.iter() {
        for batch in partition.iter() {
            rows += batch.num_rows();
        }
    }
    debug!("average {} event(s) per 1 user", rows as i64 / total_users);

    Ok(result)
}

