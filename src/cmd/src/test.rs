use std::fs;
use std::net::SocketAddr;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Decimal128Builder;
use arrow::array::Int16Builder;
use arrow::array::Int32Builder;
use arrow::array::Int64Builder;
use arrow::array::Int8Builder;
use arrow::array::StringBuilder;
use arrow::array::TimestampNanosecondBuilder;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::Router;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use chrono::Utc;
use clap::Parser;
use common::types::DType;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::datasource::TableProvider;
use hyper::Server;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::test_util::create_event;
use metadata::test_util::create_property;
use metadata::test_util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use platform::auth;
use query::datasources::local::LocalTable;
use query::QueryProvider;
use scan_dir::ScanDir;
use storage::db::OptiDBImpl;
use storage::db::Options;
use storage::NamedValue;
use storage::Value;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::info;

use crate::init_metrics;
use crate::init_project;
use crate::init_system;

#[derive(Parser, Clone)]
pub struct Test {
    #[arg(long)]
    path: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
    #[arg(long)]
    out_parquet: PathBuf,
    #[arg(long)]
    partitions: Option<usize>,
    #[arg(long, default_value = "4096")]
    batch_size: usize,
}

struct Builders {
    b_project_id: Int64Builder,
    b_user_id: Int64Builder,
    b_created_at: TimestampNanosecondBuilder,
    b_event: Int64Builder,
    b_i8: Int8Builder,
    b_i16: Int16Builder,
    b_i32: Int32Builder,
    b_i64: Int64Builder,
    b_b: BooleanBuilder,
    b_str: StringBuilder,
    b_ts: TimestampNanosecondBuilder,
    b_dec: Decimal128Builder,
    b_group: Int64Builder,
    b_v: Int64Builder,
}

impl Builders {
    pub fn new() -> Self {
        Self {
            b_project_id: Int64Builder::new(),
            b_user_id: Int64Builder::new(),
            b_created_at: TimestampNanosecondBuilder::new(),
            b_event: Int64Builder::new(),
            b_i8: Int8Builder::new(),
            b_i16: Int16Builder::new(),
            b_i32: Int32Builder::new(),
            b_i64: Int64Builder::new(),
            b_b: BooleanBuilder::new(),
            b_str: StringBuilder::new(),
            b_ts: TimestampNanosecondBuilder::new(),
            b_dec: Decimal128Builder::new()
                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                .unwrap(),
            b_group: Int64Builder::new(),
            b_v: Int64Builder::new(),
        }
    }

    pub fn finish(&mut self, schema: SchemaRef) -> RecordBatch {
        let arrs = {
            vec![
                Arc::new(self.b_project_id.finish()) as ArrayRef,
                Arc::new(self.b_user_id.finish()) as ArrayRef,
                Arc::new(self.b_created_at.finish()) as ArrayRef,
                Arc::new(self.b_event.finish()) as ArrayRef,
                Arc::new(self.b_i8.finish()) as ArrayRef,
                Arc::new(self.b_i16.finish()) as ArrayRef,
                Arc::new(self.b_i32.finish()) as ArrayRef,
                Arc::new(self.b_i64.finish()) as ArrayRef,
                Arc::new(self.b_b.finish()) as ArrayRef,
                Arc::new(self.b_str.finish()) as ArrayRef,
                Arc::new(self.b_ts.finish()) as ArrayRef,
                Arc::new(self.b_dec.finish()) as ArrayRef,
                Arc::new(self.b_group.finish()) as ArrayRef,
                Arc::new(self.b_v.finish()) as ArrayRef,
            ]
        };
        RecordBatch::try_new(schema, arrs).unwrap()
    }

    pub fn len(&self) -> usize {
        self.b_user_id.len()
    }
}

pub fn gen_mem(
    partitions: usize,
    batch_size: usize,
    db: &Arc<OptiDBImpl>,
    proj_id: u64,
) -> Result<Vec<Vec<RecordBatch>>, anyhow::Error> {
    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;
    let mut res = vec![Vec::new(); partitions];

    let mut builders = Vec::new();
    for _p in 0..partitions {
        builders.push(Builders::new());
    }
    let users = 1;
    let days = 1;
    let events = 2;
    for user in 0..users {
        let partition = user % partitions;
        let mut cur_time = now - Duration::days(days);
        for _day in 0..days {
            let mut event_time = cur_time;
            for event in 0..events {
                builders[partition]
                    .b_project_id
                    .append_value(proj_id as i64);
                builders[partition].b_user_id.append_value(user as i64);
                builders[partition]
                    .b_created_at
                    .append_value(event_time.timestamp_nanos_opt().unwrap());
                builders[partition].b_event.append_value(1);
                builders[partition].b_i8.append_value(event as i8);
                builders[partition].b_i16.append_value(event as i16);
                builders[partition].b_i32.append_value(event as i32);
                builders[partition].b_i64.append_value(event);
                builders[partition].b_b.append_value(event % 2 == 0);
                builders[partition]
                    .b_str
                    .append_value(format!("event {}", event).as_str());
                builders[partition].b_ts.append_value(event * 1000);
                builders[partition].b_dec.append_value(
                    event as i128 * 10_i128.pow(DECIMAL_SCALE as u32) + event as i128 * 100,
                );
                builders[partition]
                    .b_group
                    .append_value(event % (events / 2));
                // two group of users with different "v" value to proper integration tests
                if user % 2 == 0 {
                    builders[partition].b_v.append_value(event);
                } else {
                    builders[partition].b_v.append_value(event * 2);
                }

                if builders[partition].len() >= batch_size {
                    let batch = builders[partition].finish(Arc::new(db.schema1("events")?.clone()));
                    res[partition].push(batch);
                }

                let d = Duration::days(1).num_seconds() / events;
                let diff = Duration::seconds(d);
                event_time = event_time.add(diff);
            }
            cur_time = cur_time.add(Duration::days(1));
        }
    }

    for (partition, batches) in res.iter_mut().enumerate() {
        if builders[partition].len() > 0 {
            let batch = builders[partition].finish(Arc::new(db.schema1("events")?.clone()));
            batches.push(batch);
        }
    }

    Ok(res)
}

pub async fn gen(args: &Test) -> Result<(), anyhow::Error> {
    fs::remove_dir_all(&args.path).unwrap();
    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let db = Arc::new(OptiDBImpl::open(args.path.join("store"), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, args.partitions.unwrap_or_else(num_cpus::get))?;

    info!("creating org structure and admin account...");
    let proj_id = crate::init_test_org_structure(&md)?;

    info!("project initialization...");
    init_project(proj_id, &md)?;

    info!("starting sample data generation...");
    let _partitions = args.partitions.unwrap_or_else(num_cpus::get);

    let props = [
        ("i_8", DType::Int8),
        ("i_16", DType::Int16),
        ("i_32", DType::Int32),
        ("i_64", DType::Int64),
        ("ts", DType::Timestamp),
        ("bool", DType::Boolean),
        ("bool_nullable", DType::Boolean),
        ("string", DType::String),
        ("decimal", DType::Decimal),
        ("group", DType::Int64),
        ("v", DType::Int64),
    ];

    for (name, dt) in props {
        create_property(&md, proj_id, CreatePropertyMainRequest {
            name: name.to_string(),
            typ: Type::System, // do this to keep property names as is
            data_type: dt,
            nullable: true,
            dict: None,
        })?;
    }

    create_property(&md, proj_id, CreatePropertyMainRequest {
        name: "string_dict".to_string(),
        typ: Type::System,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int8),
    })?;

    md.dictionaries
        .get_key_or_create(1, "string_dict", "привет")?;
    md.dictionaries.get_key_or_create(1, "string_dict", "мир")?;
    let e = create_event(&md, proj_id, "event".to_string())?;
    md.dictionaries
        .get_key_or_create(proj_id, "event", "event")?;

    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;

    let users = 10;
    let days = 50;
    let events = 20;
    let mut vals: Vec<NamedValue> = vec![];
    let mut i = 0;
    for user in 0..users {
        let mut cur_time = now - Duration::days(days);
        for _day in 0..days {
            let mut event_time = cur_time;
            for event in 0..events {
                vals.truncate(0);
                vals.push(NamedValue::new(
                    "project_id".to_string(),
                    Value::Int64(Some(proj_id as i64)),
                ));
                vals.push(NamedValue::new(
                    "user_id".to_string(),
                    Value::Int64(Some(user as i64 + 1)),
                ));
                vals.push(NamedValue::new(
                    "created_at".to_string(),
                    Value::Timestamp(Some(event_time.timestamp_nanos_opt().unwrap())),
                ));
                vals.push(NamedValue::new(
                    "event".to_string(),
                    Value::Int64(Some(e.id as i64)),
                ));
                vals.push(NamedValue::new(
                    "event_id".to_string(),
                    Value::Int64(Some(i)),
                ));
                vals.push(NamedValue::new(
                    "i_8".to_string(),
                    Value::Int8(Some(event as i8)),
                ));
                vals.push(NamedValue::new(
                    "i_16".to_string(),
                    Value::Int16(Some(event as i16)),
                ));
                vals.push(NamedValue::new(
                    "i_32".to_string(),
                    Value::Int32(Some(event as i32)),
                ));
                vals.push(NamedValue::new(
                    "i_64".to_string(),
                    Value::Int64(Some(event)),
                ));
                vals.push(NamedValue::new(
                    "ts".to_string(),
                    Value::Timestamp(Some(event)),
                ));
                vals.push(NamedValue::new(
                    "bool".to_string(),
                    Value::Boolean(Some(event % 3 == 0)),
                ));

                let bv = match event % 3 {
                    0 => Some(true),
                    1 => Some(false),
                    2 => None,
                    _ => unimplemented!(),
                };

                vals.push(NamedValue::new(
                    "bool_nullable".to_string(),
                    Value::Boolean(bv),
                ));

                if event % 3 == 0 {
                    vals.push(NamedValue::new(
                        "string".to_string(),
                        Value::String(Some("привет".to_string())),
                    ));

                    vals.push(NamedValue::new(
                        "string_dict".to_string(),
                        Value::Int8(Some(1)),
                    ));
                } else {
                    vals.push(NamedValue::new(
                        "string".to_string(),
                        Value::String(Some("мир".to_string())),
                    ));

                    vals.push(NamedValue::new(
                        "string_dict".to_string(),
                        Value::Int8(Some(2)),
                    ));
                }
                vals.push(NamedValue::new(
                    "decimal".to_string(),
                    Value::Decimal(Some(
                        event as i128 * 10_i128.pow(DECIMAL_SCALE as u32) + event as i128 * 100,
                    )),
                ));
                vals.push(NamedValue::new(
                    "group".to_string(),
                    Value::Int64(Some(event % (events / 2))),
                ));
                // two group of users with different "v" value to proper integration tests
                // event value
                if user % 3 == 0 {
                    vals.push(NamedValue::new("v".to_string(), Value::Int64(Some(event))));
                } else {
                    vals.push(NamedValue::new(
                        "v".to_string(),
                        Value::Int64(Some(event * 2)),
                    ));
                }

                db.insert("events", vals.clone())?;
                let d = Duration::days(1).num_seconds() / events;
                let diff = Duration::seconds(d);
                event_time = event_time.add(diff);
                i += 1;
            }
            cur_time = cur_time.add(Duration::days(1));
        }
    }
    db.flush()?;

    info!("successfully generated!");
    let data_provider: Arc<dyn TableProvider> =
        Arc::new(LocalTable::try_new(db.clone(), "events".to_string())?);

    let all_parquet_files: Vec<_> = ScanDir::files()
        .walk(args.path.join("store/tables/events"), |iter| {
            iter.filter(|(_, name)| name.ends_with(".parquet"))
                .map(|(ref entry, _)| entry.path())
                .collect()
        })
        .unwrap();

    for ppath in all_parquet_files {
        fs::copy(
            ppath.clone(),
            args.out_parquet.join(ppath.file_name().unwrap()),
        )?;
    }

    let query_provider = Arc::new(QueryProvider::try_new_from_provider(
        md.clone(),
        db.clone(),
        data_provider,
    )?);

    let auth_cfg = auth::provider::Config {
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

    let mut router = Router::new();
    info!("attaching platform routes...");
    router = platform::http::attach_routes(router, &md, &platform_provider, auth_cfg, None);
    info!("listening on {}", args.host);
    let server = Server::bind(&args.host).serve(router.into_make_service());
    let graceful = server.with_graceful_shutdown(async {
        let mut sig_int =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("failed to install signal");
        let mut sig_term =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("failed to install signal");
        select! {
            _=sig_int.recv()=>info!("SIGINT received"),
            _=sig_term.recv()=>info!("SIGTERM received"),
        }
    });

    Ok(graceful.await?)
}
