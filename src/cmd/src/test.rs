use std::fmt::Write;
use std::fs;
use std::net::SocketAddr;
use std::ops::Add;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use chrono::Utc;
use clap::Parser;
use common::config::Config;
use common::types::DType;
use common::types::TABLE_EVENTS;
use common::DECIMAL_SCALE;
use hyper::Server;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::util::create_event;
use metadata::util::create_property;
use metadata::util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use platform::auth;
use platform::projects::init_project;
use scan_dir::ScanDir;
use storage::db::OptiDBImpl;
use storage::db::Options;
use storage::NamedValue;
use storage::Value;
use tokio::net::TcpListener;
use tokio::select;
use tokio::signal::unix::SignalKind;
use tracing::info;
use query::event_records::EventRecordsProvider;
use query::event_segmentation::EventSegmentationProvider;
use query::funnel::FunnelProvider;
use query::group_records::GroupRecordsProvider;
use query::properties::PropertiesProvider;

use crate::init_metrics;
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

pub async fn gen(args: &Test) -> Result<(), anyhow::Error> {
    fs::remove_dir_all(&args.path).unwrap();
    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let db = Arc::new(OptiDBImpl::open(args.path.join("store"), Options {})?);
    let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
    info!("metrics initialization...");
    init_metrics();
    info!("system initialization...");
    init_system(&md, &db, num_cpus::get())?;

    info!("creating org structure and admin account...");
    let proj = crate::init_test_org_structure(&md)?;

    info!("project initialization...");
    init_project(proj.id, &md)?;

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
        create_property(&md, proj.id, CreatePropertyMainRequest {
            name: name.to_string(),
            display_name: None,
            typ: Type::System, // do this to keep property names as is
            data_type: dt,
            nullable: true,
            hidden: false,
            dict: None,
            is_system: true,
        })?;
    }

    create_property(&md, proj.id, CreatePropertyMainRequest {
        name: "string_dict".to_string(),
        display_name: None,
        typ: Type::System,
        data_type: DType::String,
        nullable: true,
        hidden: false,
        dict: Some(DictionaryType::Int8),
        is_system:  true,
    })?;

    md.dictionaries
        .get_key_or_create(1, "string_dict", "привет")?;
    md.dictionaries.get_key_or_create(1, "string_dict", "мир")?;
    let e = create_event(&md, proj.id, "event".to_string())?;
    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;

    let users = 10;
    let days = 365;
    let events = 20;
    let total = (users * days * events) as u64;
    let records_per_parquet = total as i64 / 3;
    let pb = ProgressBar::new(total);

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

    let mut vals: Vec<NamedValue> = vec![];
    let mut i = 0;
    for _ in 0..2 {
        for user in 0..users {
            let mut cur_time = now - Duration::days(days);
            for _day in 0..=days {
                let mut event_time = cur_time;
                for event in 0..=events {
                    vals.truncate(0);
                    vals.push(NamedValue::new(
                        "project_id".to_string(),
                        Value::Int64(Some(proj.id as i64)),
                    ));
                    vals.push(NamedValue::new(
                        "user_id".to_string(),
                        Value::Int64(Some(user + 1)),
                    ));
                    vals.push(NamedValue::new(
                        "created_at".to_string(),
                        Value::Timestamp(Some(event_time.timestamp_millis())),
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

                    db.insert(TABLE_EVENTS, vals.clone())?;
                    let d = Duration::days(1).num_seconds() / events;
                    let diff = Duration::seconds(d);
                    event_time = event_time.add(diff);
                    i += 1;
                    pb.inc(1);
                    if i >= records_per_parquet {
                        i = 0;
                        db.flush(TABLE_EVENTS)?;
                    }
                }
                cur_time = cur_time.add(Duration::days(1));
            }
        }
    }
    db.flush(TABLE_EVENTS)?;
    info!("successfully generated {i} events!");
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

    let cfg = Config::default();
    let es_prov = Arc::new(EventSegmentationProvider::new(md.clone(), db.clone()));
    let funnel_prov = Arc::new(FunnelProvider::new(md.clone(), db.clone()));
    let prop_prov = Arc::new(PropertiesProvider::new(md.clone(), db.clone()));
    let er_prov = Arc::new(EventRecordsProvider::new(md.clone(), db.clone()));
    let gr_prov = Arc::new(GroupRecordsProvider::new(md.clone(), db.clone()));

    let platform_provider = Arc::new(platform::PlatformProvider::new(
        md.clone(),
        es_prov,
        funnel_prov,
        prop_prov,
        er_prov,
        gr_prov,
        cfg.clone(),
    ));

    let mut router = Router::new();
    info!("attaching platform routes...");
    router = platform::http::attach_routes(router, &md, &platform_provider, cfg);
    info!("listening on {}", args.host);
    let signal = async {
        let mut sig_int =
            tokio::signal::unix::signal(SignalKind::interrupt()).expect("failed to install signal");
        let mut sig_term =
            tokio::signal::unix::signal(SignalKind::terminate()).expect("failed to install signal");
        select! {
            _=sig_int.recv()=>info!("SIGINT received"),
            _=sig_term.recv()=>info!("SIGTERM received"),
        }
    };
    Ok(axum::serve(TcpListener::bind(&args.host).await?, router)
        .with_graceful_shutdown(signal)
        .await?)
}
