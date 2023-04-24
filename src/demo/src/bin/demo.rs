use std::env::temp_dir;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytesize::ByteSize;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use datafusion::datasource::MemTable;
use dateparser::DateTimeUtc;
use demo::error::DemoError;
use demo::store;
use demo::store::gen;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::store::Store;
use metadata::MetadataProvider;
use platform::auth;
use platform::auth::password::make_password_hash;
use query::ProviderImpl;
use service::tracing::TracingCliArgs;
use tracing::debug;
use tracing::info;
use uuid::Uuid;

extern crate parse_duration;

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    tracing: TracingCliArgs,

    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long)]
    demo_data_path: PathBuf,
    #[arg(long)]
    md_path: Option<PathBuf>,
    #[arg(long)]
    ui_path: Option<PathBuf>,
    #[arg(long, default_value = "365 days")]
    duration: Option<String>,
    #[arg(long)]
    to_date: Option<String>,
    #[arg(long, default_value = "10")]
    new_daily_users: usize,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    args.tracing.init()?;

    let to_date = match &args.to_date {
        None => Utc::now(),
        Some(dt) => dt.parse::<DateTimeUtc>()?.0.with_timezone(&Utc),
    };

    let duration = Duration::from_std(parse_duration::parse(args.duration.unwrap().as_str())?)?;
    let from_date = to_date - duration;
    let md_path = match args.md_path {
        None => temp_dir().join(format!("{}.db", Uuid::new_v4())),
        Some(path) => {
            if !path.try_exists()? {
                return Err(DemoError::FileNotFound(format!(
                    "metadata path {path:?} doesn't exist"
                ))
                .into());
            }

            path
        }
    };

    if let Some(ui_path) = &args.ui_path {
        if !ui_path.try_exists()? {
            return Err(
                DemoError::FileNotFound(format!("ui path {ui_path:?} doesn't exist")).into(),
            );
        }
    }

    if !args.demo_data_path.try_exists()? {
        return Err(DemoError::FileNotFound(format!(
            "demo data path {:?} doesn't exist",
            args.demo_data_path
        ))
        .into());
    }

    let store = Arc::new(Store::new(md_path.clone()));
    let md = Arc::new(MetadataProvider::try_new(store)?);

    info!("creating org structure and admin account...");
    {
        let admin = md
            .accounts
            .create(CreateAccountRequest {
                created_by: None,
                password_hash: make_password_hash("admin")?,
                email: "admin@email.com".to_string(),
                first_name: Some("admin".to_string()),
                last_name: None,
                role: Some(Role::Admin),
                organizations: None,
                projects: None,
                teams: None,
            })
            .await?;

        let org = md
            .organizations
            .create(CreateOrganizationRequest {
                created_by: admin.id,
                name: "Test Organization".to_string(),
            })
            .await?;

        let proj1 = md
            .projects
            .create(org.id, CreateProjectRequest {
                created_by: admin.id,
                name: "Test Project".to_string(),
            })
            .await?;

        let user = md
            .accounts
            .create(CreateAccountRequest {
                created_by: Some(admin.id),
                password_hash: make_password_hash("test")?,
                email: "user@test.com".to_string(),
                first_name: Some("user".to_string()),
                last_name: None,
                role: None,
                organizations: Some(vec![(org.id, OrganizationRole::Member)]),
                projects: Some(vec![(proj1.id, ProjectRole::Member)]),
                teams: None,
            })
            .await?;
    }
    info!("starting demo instance...");
    debug!("metadata path: {:?}", md_path);
    debug!("demo data path: {:?}", args.demo_data_path);
    if args.ui_path.is_some() {
        debug!("ui path: {:?}", args.ui_path);
    }
    debug!("from date {}", from_date);
    let date_diff = to_date - from_date;
    debug!("to date {}", to_date);
    debug!(
        "time range: {}",
        humantime::format_duration(date_diff.to_std()?)
    );
    debug!("new daily users: {}", args.new_daily_users);
    let total_users = args.new_daily_users as i64 * date_diff.num_days();
    info!("expecting total unique users: {total_users}");
    info!("starting sample data generation...");

    let batches = {
        let store_cfg = store::Config {
            org_id: 1,
            project_id: 1,
            md: md.clone(),
            from_date,
            to_date,
            products_rdr: File::open(args.demo_data_path.join("products.csv"))
                .map_err(|err| DemoError::Internal(format!("can't open products.csv: {err}")))?,
            geo_rdr: File::open(args.demo_data_path.join("geo.csv"))
                .map_err(|err| DemoError::Internal(format!("can't open geo.csv: {err}")))?,
            device_rdr: File::open(args.demo_data_path.join("device.csv"))
                .map_err(|err| DemoError::Internal(format!("can't open device.csv: {err}")))?,
            new_daily_users: args.new_daily_users,
            batch_size: 4096,
            partitions: num_cpus::get(),
        };

        gen(store_cfg).await?
    };

    info!("successfully generated!");
    let mut rows: usize = 0;
    let mut data_size_bytes: usize = 0;
    for partition in batches.iter() {
        for batch in partition.iter() {
            rows += batch.num_rows();
            for column in batch.columns() {
                data_size_bytes += column.get_array_memory_size();
            }
        }
    }
    debug!(
        "partitions: {}, batches: {}",
        batches.len(),
        batches[0].len()
    );
    debug!("average {} event(s) per 1 user", rows as i64 / total_users);
    debug!(
        "uncompressed dataset in-memory size: {}",
        ByteSize::b(data_size_bytes as u64)
    );

    let data_provider = Arc::new(MemTable::try_new(batches[0][0].schema(), batches)?);
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
