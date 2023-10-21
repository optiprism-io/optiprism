use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::Duration;
use clap::Parser;
use common::types::USER_COLUMN_CLIENT_FAMILY;
use common::types::USER_COLUMN_CLIENT_VERSION_MAJOR;
use common::types::USER_COLUMN_CLIENT_VERSION_MINOR;
use common::types::USER_COLUMN_CLIENT_VERSION_PATCH;
use common::types::USER_COLUMN_DEVICE_BRAND;
use common::types::USER_COLUMN_DEVICE_FAMILY;
use common::types::USER_COLUMN_DEVICE_MODEL;
use common::types::USER_COLUMN_OS_FAMILY;
use common::types::USER_COLUMN_OS_VERSION_MAJOR;
use common::types::USER_COLUMN_OS_VERSION_MINOR;
use common::types::USER_COLUMN_OS_VERSION_PATCH;
use common::types::USER_COLUMN_OS_VERSION_PATCH_MINOR;
use futures::executor::block_on;
use ingester::executor::Executor;
use ingester::processors::user_agent::identify;
use ingester::processors::user_agent::track;
use ingester::sources;
use ingester::Destination;
use ingester::Identify;
use ingester::Processor;
use ingester::Track;
use metadata::events;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Provider;
use metadata::properties::Status;
use metadata::store::Store;
use metadata::MetadataProvider;
use service::tracing::TracingCliArgs;
use tracing::info;

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long)]
    md_path: PathBuf,
    #[arg(long)]
    ua_path: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    args.tracing.init()?;

    info!("starting http instance...");

    let store = Arc::new(Store::new(args.md_path.clone()));
    let user_props = Arc::new(properties::ProviderImpl::new_user(store.clone()));

    // todo move somewhere else
    {
        let props = vec![
            USER_COLUMN_CLIENT_FAMILY,
            USER_COLUMN_CLIENT_VERSION_MINOR,
            USER_COLUMN_CLIENT_VERSION_MAJOR,
            USER_COLUMN_CLIENT_VERSION_PATCH,
            USER_COLUMN_DEVICE_FAMILY,
            USER_COLUMN_DEVICE_BRAND,
            USER_COLUMN_DEVICE_MODEL,
            USER_COLUMN_OS_FAMILY,
            USER_COLUMN_OS_VERSION_MAJOR,
            USER_COLUMN_OS_VERSION_MINOR,
            USER_COLUMN_OS_VERSION_PATCH,
            USER_COLUMN_OS_VERSION_PATCH_MINOR,
        ];
        for prop in props {
            block_on(user_props.create(1, 1, CreatePropertyRequest {
                created_by: 1,
                tags: None,
                name: prop.to_string(),
                description: None,
                display_name: None,
                typ: DataType::Utf8,
                status: Status::Enabled,
                is_system: true,
                nullable: false,
                is_array: false,
                is_dictionary: true,
                dictionary_type: Some(DataType::Int64),
            }));
        }
    }

    let event_props = Arc::new(properties::ProviderImpl::new_event(store.clone()));

    let events = Arc::new(events::ProviderImpl::new(store.clone()));
    let mut track_processors = Vec::new();
    let ua = track::UserAgent::try_new(user_props.clone(), File::open(args.ua_path.clone())?)?;
    track_processors.push(Arc::new(ua) as Arc<dyn Processor<Track>>);

    let mut track_destinations = Vec::new();
    let track_debug_dst = ingester::destinations::debug::track::Debug::new();
    track_destinations.push(Arc::new(track_debug_dst) as Arc<dyn Destination<Track>>);
    let track_exec = Executor::<Track>::new(
        track_processors.clone(),
        track_destinations.clone(),
        event_props.clone(),
        user_props.clone(),
        events.clone(),
    );

    let mut identify_processors = Vec::new();
    let ua = identify::UserAgent::try_new(user_props.clone(), File::open(args.ua_path)?)?;
    identify_processors.push(Arc::new(ua) as Arc<dyn Processor<Identify>>);

    let mut identify_destinations = Vec::new();
    let identify_debug_dst = ingester::destinations::debug::identify::Debug::new();
    identify_destinations.push(Arc::new(identify_debug_dst) as Arc<dyn Destination<Identify>>);
    let identify_exec = Executor::<Identify>::new(
        identify_processors,
        identify_destinations,
        event_props.clone(),
        user_props.clone(),
        events.clone(),
    );
    let svc = sources::http::service::Service::new(track_exec, identify_exec, args.host);

    info!("start listening on {}", args.host);

    Ok(svc.serve().await?)
}
