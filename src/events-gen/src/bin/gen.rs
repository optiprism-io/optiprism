use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

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
use events_gen::profiles::ProfileProvider;
use events_gen::store::products::ProductProvider;
use events_gen::store::scenario;
use events_gen::store::scenario::Scenario;
use events_gen::store_dictionary::events::Event;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::store::Store;
use metadata::MetadataProvider;
use rand::thread_rng;
use service::tracing::TracingCliArgs;
use tracing::debug;
use tracing::info;

#[derive(Debug)]
enum Format {
    CSV,
    TSV,
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
        File::open(args.demo_data_path.join("products.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open products.csv: {err}")))?,
        File::open(args.demo_data_path.join("device.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open device.csv: {err}")))?,
    )?;
    info!("loading products...");
    let products = ProductProvider::try_new_from_csv(
        org_id,
        project_id,
        &mut rng,
        File::open(args.demo_data_path.join("products.csv"))
            .map_err(|err| EventsGenError::Internal(format!("can't open products.csv: {err}")))?,
    )
    .await?;

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

    info!("generating events...");
    let run_cfg = scenario::Config {
        rng: rng.clone(),
        gen,
        schema: schema.clone(),
        products,
        to: to_date,
        batch_size: 4096,
        partitions: num_cpus::get(),
    };

    let mut scenario = Scenario::new(run_cfg);
    let batches = scenario.run().await?;

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

    Ok(())
}
