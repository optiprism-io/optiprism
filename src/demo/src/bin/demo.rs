
use std::env::temp_dir;
use std::fmt::Write;
use std::fs::File;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::ArrayRef;
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
use datafusion::datasource::MemTable;
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
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::provider_impl::Namespace;
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
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    #[arg(long)]
    md_path: Option<PathBuf>,
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long, global = true)]
    out_parquet: Option<PathBuf>,
    #[arg(long, global = true)]
    partitions: Option<usize>,
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

    let md_path = match args.md_path.clone() {
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
    debug!("metadata path: {:?}", md_path);

    let store = Arc::new(Store::new(md_path.clone()));
    let md = Arc::new(MetadataProvider::try_new(store)?);

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

        let _user = md
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

    let partitions = match &args.command {
        Some(cmd) => match cmd {
            Commands::Shop(shop) => gen_store(&args, shop, &md).await?,
            Commands::Test { .. } => gen_test(&md).await?,
        },
        _ => unreachable!(),
    };

    info!("successfully generated!");
    let mut rows: usize = 0;
    let mut data_size_bytes: usize = 0;
    for partition in partitions.iter() {
        for batch in partition.iter() {
            rows += batch.num_rows();
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

    let schema = partitions[0][0].schema().clone();
    if let Some(path) = args.out_parquet {
        write_parquet(1, 1, &md, &path, &partitions, &schema)?;
    }

    let data_provider = Arc::new(MemTable::try_new(partitions[0][0].schema(), partitions)?);
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

async fn gen_test(md: &Arc<MetadataProvider>) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
    info!("starting sample data generation...");
    test::gen(md, 1, 1).await
}

async fn gen_store(
    args: &Cli,
    cmd_args: &Shop,
    md: &Arc<MetadataProvider>,
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
        md: md.clone(),
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
        partitions: args.partitions.unwrap_or_else(|| num_cpus::get()),
    };

    let result = shop::gen(&md, store_cfg).await?;
    let mut rows: usize = 0;
    for partition in result.iter() {
        for batch in partition.iter() {
            rows += batch.num_rows();
        }
    }
    debug!("average {} event(s) per 1 user", rows as i64 / total_users);

    return Ok(result);
}

fn write_parquet(
    org_id: u64,
    project_id: u64,
    md: &Arc<MetadataProvider>,
    path: &PathBuf,
    partitions: &Vec<Vec<RecordBatch>>,
    schema: &SchemaRef,
) -> Result<(), anyhow::Error> {
    info!("converting dictionaries to raw...");

    let num_events = partitions
        .iter()
        .map(|v| v.iter().map(|v| v.num_rows()).sum::<usize>())
        .sum::<usize>();

    let pb = ProgressBar::new(num_events as u64);

    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} events ({eta})",
        )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
    );

    let event_props = block_on(md.event_properties.list(org_id, project_id))?.data;
    let user_props = block_on(md.user_properties.list(org_id, project_id))?.data;
    let mut out_fields = vec![];
    for field in schema.fields().iter() {
        if field.name().starts_with("event_") {
            let prop = event_props
                .iter()
                .find(|p| p.column_name(Namespace::Event) == *field.name())
                .unwrap();
            out_fields.push(Field::new(
                prop.column_name(Namespace::Event),
                prop.typ.clone(),
                prop.nullable,
            ));
        } else if field.name().starts_with("user_") {
            let prop = user_props
                .iter()
                .find(|p| p.column_name(Namespace::User) == *field.name())
                .unwrap();
            out_fields.push(Field::new(
                prop.column_name(Namespace::User),
                prop.typ.clone(),
                prop.nullable,
            ));
        } else {
            unimplemented!("{}", field.name())
        };
    }

    let out_schema = Arc::new(Schema::new(out_fields));
    for (pid, partition) in partitions.iter().enumerate() {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let path = path.join(format!("{}.parquet", pid));
        let file = File::create(path.clone())?;
        let mut writer = ArrowWriter::try_new(file, out_schema.clone(), Some(props)).unwrap();

        for batch in partition {
            let mut cols = vec![];
            let mut fields = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let prop = if field.name().starts_with("event_") {
                    event_props
                        .iter()
                        .find(|p| p.column_name(Namespace::Event) == *field.name())
                        .unwrap()
                } else if field.name().starts_with("user_") {
                    user_props
                        .iter()
                        .find(|p| p.column_name(Namespace::User) == *field.name())
                        .unwrap()
                } else {
                    unimplemented!("{}", field.name())
                };

                if let Some(typ) = &prop.dictionary_type {
                    let mut b = StringBuilder::with_capacity(100, 100 * batch.columns()[idx].len());

                    let field = Field::new(field.name(), DataType::Utf8, field.is_nullable());
                    fields.push(field.clone());
                    let arr = match typ {
                        DataType::UInt8 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<UInt8Array>()
                                .unwrap();

                            for v in arr {
                                if let Some(i) = v {
                                    // TODO make dict cache
                                    let s = block_on(md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    ))?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DataType::UInt16 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<UInt16Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = block_on(md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    ))?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DataType::UInt32 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<UInt32Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = block_on(md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    ))?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DataType::UInt64 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = block_on(md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i,
                                    ))?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        _ => unimplemented!("dictionary type {:?} is unsupported", prop.typ),
                    };

                    cols.push(arr);
                } else {
                    let field = Field::new(
                        field.name(),
                        prop.dictionary_type
                            .clone()
                            .unwrap_or_else(|| prop.typ.clone()),
                        field.is_nullable(),
                    );
                    fields.push(field.clone());

                    cols.push(batch.columns()[idx].clone());
                }
            }

            let out_schema = Arc::new(Schema::new(fields.clone()));
            let out_batch = RecordBatch::try_new(out_schema.clone(), cols.clone())?;
            pb.inc(out_batch.num_rows() as u64);

            writer.write(&out_batch)?;
        }
        writer.close()?;
    }

    Ok(())
}
