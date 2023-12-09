use std::collections::HashMap;
use std::env::temp_dir;
use std::fmt::Write;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::array::ArrayRef;
use arrow::array::StringBuilder;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytesize::ByteSize;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use dateparser::DateTimeUtc;
use enum_iterator::all;
use events_gen::error::EventsGenError;
use events_gen::generator;
use events_gen::generator::Generator;
use events_gen::store::events::Event;
use events_gen::store::products::ProductProvider;
use events_gen::store::profiles::ProfileProvider;
use events_gen::store::scenario;
use events_gen::store::scenario::Scenario;
use events_gen::store::schema::create_entities;
use futures::executor::block_on;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use platform::auth::password::make_password_hash;
// use rand::ableRng;
use rand::thread_rng;
use service::tracing::TracingCliArgs;
use tracing::debug;
use tracing::info;
use uuid::Uuid;
use store::db::{OptiDBImpl, Options, TableOptions};

#[derive(clap::ValueEnum, Clone, Debug, Default)]
enum Format {
    Csv,
    Tsv,
    #[default]
    Parquet,
}

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long)]
    demo_data_path: PathBuf,
    #[arg(long)]
    path: PathBuf,
    #[arg(long, default_value = "365 days")]
    duration: Option<String>,
    #[arg(long)]
    to_date: Option<String>,
    #[arg(long, default_value = "10")]
    new_daily_users: usize,
    #[arg(long)]
    out_path: PathBuf,
    #[arg(long, value_enum, default_value = "csv")]
    format: Format,
    #[arg(long)]
    partitions: Option<usize>,
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
    if !args.path.try_exists()? {
        return Err(EventsGenError::FileNotFound(format!(
            "metadata path {:?} doesn't exist",args.path
        ))
            .into());
    }

    if !args.demo_data_path.try_exists()? {
        return Err(EventsGenError::General(format!(
            "demo data path {:?} doesn't exist",
            args.demo_data_path
        ))
            .into());
    }

    if !args.out_path.try_exists()? {
        return Err(
            EventsGenError::General(format!("out path {:?} doesn't exist", args.out_path)).into(),
        );
    }

    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let md = Arc::new(MetadataProvider::try_new(rocks)?);
    let db = Arc::new(OptiDBImpl::open(args.path.join("store"), Options {})?);
    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        partitions: args.partitions.unwrap_or_else(num_cpus::get),
        index_cols: 2,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024 * 100,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024*8*8,
    };
    db.create_table("events",topts)?;
    let schema = db.schema1("events")?;
    info!("creating org structure and admin account...");
    {
        let admin = md.accounts.create(CreateAccountRequest {
            created_by: None,
            password_hash: make_password_hash("admin")?,
            email: "admin@email.com".to_string(),
            first_name: Some("admin".to_string()),
            last_name: None,
            role: Some(Role::Admin),
            organizations: None,
            projects: None,
            teams: None,
        })?;

        let org = md.organizations.create(CreateOrganizationRequest {
            created_by: admin.id,
            name: "Test Organization".to_string(),
        })?;

        let proj1 = md.projects.create(org.id, CreateProjectRequest {
            created_by: admin.id,
            name: "Test Project".to_string(),
        })?;

        let _user = md.accounts.create(CreateAccountRequest {
            created_by: Some(admin.id),
            password_hash: make_password_hash("test")?,
            email: "user@test.com".to_string(),
            first_name: Some("user".to_string()),
            last_name: None,
            role: None,
            organizations: Some(vec![(org.id, OrganizationRole::Member)]),
            projects: Some(vec![(proj1.id, ProjectRole::Member)]),
            teams: None,
        })?;
    }

    debug!("db path: {:?}", args.path);
    debug!("demo data path: {:?}", args.demo_data_path);
    debug!("out path: {:?} ({:?})", args.out_path, args.format);
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
    info!("starting data generation...");

    let mut rng = thread_rng();
    let project_id = 1;
    let org_id = 1;
    info!("loading profiles...");
    let profiles = ProfileProvider::try_new_from_csv(
        org_id,
        project_id,
        &md.dictionaries,
        File::open(args.demo_data_path.join("geo.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open geo.csv: {err}")))?,
        File::open(args.demo_data_path.join("device.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open device.csv: {err}")))?,
    )?;

    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(
        org_id,
        project_id,
        &mut rng,
        md.dictionaries.clone(),
        File::open(args.demo_data_path.join("products.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open products.csv: {err}")))?,
    )?;
    info!("creating entities...");
    Arc::new(create_entities(org_id, project_id, &md, &db)?);

    info!("creating generator...");
    let gen_cfg = generator::Config {
        rng: rng.clone(),
        profiles,
        from: from_date,
        to: to_date,
        new_daily_users: args.new_daily_users,
        traffic_hourly_weights: [
            0.4, 0.37, 0.39, 0.43, 0.45, 0.47, 0.52, 0.6, 0.8, 0.9, 0.85, 0.8, 0.75, 0.85, 1.,
            0.85, 0.7, 0.63, 0.62, 0.61, 0.59, 0.57, 0.48, 0.4,
        ],
    };

    let gen = Generator::new(gen_cfg);

    let mut events_map: HashMap<Event, u64> = HashMap::default();
    for event in all::<Event>() {
        let md_event = md
            .events
            .get_by_name(org_id, project_id, event.to_string().as_str())?;
        events_map.insert(event, md_event.id);
        md.dictionaries.get_key_or_create(
            org_id,
            project_id,
            "event_event",
            event.to_string().as_str(),
        )?;
    }

    info!("generating events...");
    let run_cfg = scenario::Config {
        rng: rng.clone(),
        gen,
        schema: Arc::new(db.schema1("events")?),
        events_map,
        products,
        to: to_date,
        batch_size: 4096,
        partitions: args.partitions.unwrap_or_else(num_cpus::get),
    };

    let mut scenario = Scenario::new(run_cfg);
    let partitions = scenario.run()?;

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

    debug!("average {} event(s) per 1 user", rows as i64 / total_users);
    debug!(
        "uncompressed dataset in-memory size: {}",
        ByteSize::b(data_size_bytes as u64)
    );
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

    let event_props = md.event_properties.list(org_id, project_id)?.data;
    let user_props = md.user_properties.list(org_id, project_id)?.data;
    let mut out_fields = vec![];
    for field in schema.fields().iter() {
        if field.name().starts_with("event_") {
            let prop = event_props
                .iter()
                .find(|p| p.column_name() == *field.name())
                .unwrap();
            out_fields.push(Field::new(
                prop.column_name(),
                prop.data_type.clone().try_into()?,
                prop.nullable,
            ));
        } else if field.name().starts_with("user_") {
            let prop = user_props
                .iter()
                .find(|p| p.column_name() == *field.name())
                .unwrap();
            out_fields.push(Field::new(
                prop.column_name(),
                prop.data_type.clone().try_into()?,
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

        let path = args.out_path.join(format!("{}.parquet", pid));
        let file = File::create(path.clone())?;
        info!("saving partition {pid} to {:?}...", path.clone());
        let mut writer = ArrowWriter::try_new(file, out_schema.clone(), Some(props)).unwrap();

        for batch in partition {
            let mut cols = vec![];
            let mut fields = vec![];
            for (idx, field) in schema.fields().iter().enumerate() {
                let prop = if field.name().starts_with("event_") {
                    event_props
                        .iter()
                        .find(|p| p.column_name() == *field.name())
                        .unwrap()
                } else if field.name().starts_with("user_") {
                    user_props
                        .iter()
                        .find(|p| p.column_name() == *field.name())
                        .unwrap()
                } else {
                    unimplemented!("{}", field.name())
                };

                if let Some(typ) = &prop.dictionary_type {
                    let mut b = StringBuilder::with_capacity(100, 100 * batch.columns()[idx].len());

                    let field =
                        Field::new(field.name(), datatypes::DataType::Utf8, field.is_nullable());
                    fields.push(field.clone());
                    let arr = match typ {
                        DictionaryType::Int8 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<Int8Array>()
                                .unwrap();

                            for v in arr {
                                if let Some(i) = v {
                                    let s = md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    )?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DictionaryType::Int16 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<Int16Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    )?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DictionaryType::Int32 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    )?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        DictionaryType::Int64 => {
                            let arr = batch.columns()[idx]
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            for v in arr {
                                if let Some(i) = v {
                                    let s = md.dictionaries.get_value(
                                        org_id,
                                        project_id,
                                        field.name(),
                                        i as u64,
                                    )?;
                                    b.append_value(s);
                                } else {
                                    b.append_null();
                                }
                            }

                            Arc::new(b.finish()) as ArrayRef
                        }
                        _ => unimplemented!("dictionary type {:?} is unsupported", prop.data_type),
                    };

                    cols.push(arr);
                } else {
                    let field = Field::new(
                        field.name(),
                        prop.data_type.clone().try_into()?,
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
