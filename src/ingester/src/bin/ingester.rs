use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use clap::Parser;
use ingester::executor::Executor;
use ingester::processor;
use ingester::processors::user_agent;
use ingester::sources;
use metadata::events;
use metadata::properties;
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
    let md = Arc::new(MetadataProvider::try_new(store)?);

    let mut processors = Vec::new();

    let user_props = Arc::new(properties::ProviderImpl::new_user(store.clone()));
    let event_props = Arc::new(properties::ProviderImpl::new_event(store.clone()));
    let events = Arc::new(events::ProviderImpl::new(store.clone()));
    let ua = user_agent::UserAgent::new(event_props, user_props, events, File::open(args.ua_path)?);
    processors.push(Box::new(ua) as Box<dyn processor::Processor>);

    let mut sinks = Vec::new();
    let debug_sink = ingester::sinks::debug::DebugSink::new();
    sinks.push(Box::new(debug_sink) as Box<dyn ingester::sink::Sink>);
    let exec = Executor::new(processors, sinks);
    let svc = sources::http::Service::new(exec, args.host);

    info!("start listening on {}", args.host);

    Ok(svc.serve().await?)
}
