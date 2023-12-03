use std::fmt::Debug;
use std::fmt::Formatter;
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::Duration;
use clap::Parser;
use common::rbac::Role;
use common::types::{DType, USER_PROPERTY_CITY};
use common::types::USER_PROPERTY_CLIENT_FAMILY;
use common::types::USER_PROPERTY_CLIENT_VERSION_MAJOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_MINOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_PATCH;
use common::types::USER_PROPERTY_COUNTRY;
use common::types::USER_PROPERTY_DEVICE_BRAND;
use common::types::USER_PROPERTY_DEVICE_FAMILY;
use common::types::USER_PROPERTY_DEVICE_MODEL;
use common::types::USER_PROPERTY_OS_FAMILY;
use common::types::USER_PROPERTY_OS_VERSION_MAJOR;
use common::types::USER_PROPERTY_OS_VERSION_MINOR;
use common::types::USER_PROPERTY_OS_VERSION_PATCH;
use common::types::USER_PROPERTY_OS_VERSION_PATCH_MINOR;
use futures::executor::block_on;
use ingester::error::IngesterError;
use ingester::executor::Executor;
use ingester::sources;
use ingester::transformers::geo;
use ingester::transformers::user_agent;
use ingester::transformers::user_agent::identify;
use ingester::transformers::user_agent::track;
use ingester::Destination;
use ingester::Identify;
use ingester::Track;
use ingester::Transformer;
use metadata::accounts::CreateAccountRequest;
use metadata::accounts::Provider as AccountsProvider;
use metadata::dictionaries;
use metadata::events;
use metadata::organizations::CreateOrganizationRequest;
use metadata::organizations::Provider as OrganizationProvider;
use metadata::projects;
use metadata::projects::CreateProjectRequest;
use metadata::projects::Provider as ProjectsProvider;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Provider;
use metadata::properties::Status;
use metadata::MetadataProvider;
use service::tracing::TracingCliArgs;
use store::Value;
use tracing::debug;
use tracing::info;
use uaparser::UserAgentParser;
use store::db::{OptiDBImpl, Options};

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    ua_db_path: PathBuf,
    #[arg(long)]
    geo_city_path: PathBuf,
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    args.tracing.init()?;

    info!("starting http instance...");

    let rocks = Arc::new(metadata::rocksdb::new(args.path.join("md"))?);
    let db = OptiDBImpl::open(args.path.join("store"),Options{})?;
    let user_props = Arc::new(properties::ProviderImpl::new_user(rocks.clone()));

    // todo move somewhere else
    {
        let props = vec![
            USER_PROPERTY_CLIENT_FAMILY,
            USER_PROPERTY_CLIENT_VERSION_MINOR,
            USER_PROPERTY_CLIENT_VERSION_MAJOR,
            USER_PROPERTY_CLIENT_VERSION_PATCH,
            USER_PROPERTY_DEVICE_FAMILY,
            USER_PROPERTY_DEVICE_BRAND,
            USER_PROPERTY_DEVICE_MODEL,
            USER_PROPERTY_OS_FAMILY,
            USER_PROPERTY_OS_VERSION_MAJOR,
            USER_PROPERTY_OS_VERSION_MINOR,
            USER_PROPERTY_OS_VERSION_PATCH,
            USER_PROPERTY_OS_VERSION_PATCH_MINOR,
            USER_PROPERTY_COUNTRY,
            USER_PROPERTY_CITY,
        ];
        for prop in props {
            let _ = user_props.create(1, 1, CreatePropertyRequest {
                created_by: 1,
                tags: None,
                name: prop.to_string(),
                description: None,
                display_name: None,
                typ: properties::Type::User,
                data_type: DType::String,
                status: Status::Enabled,
                is_system: true,
                nullable: false,
                is_array: false,
                is_dictionary: true,
                dictionary_type: Some(DictionaryType::Int64),
            });
        }

        let accs = Arc::new(metadata::accounts::ProviderImpl::new(rocks.clone()));
        let admin = accs.create(CreateAccountRequest {
            created_by: None,
            password_hash: "sdf".to_string(),
            email: "admin@email.com".to_string(),
            first_name: Some("admin".to_string()),
            last_name: None,
            role: Some(Role::Admin),
            organizations: None,
            projects: None,
            teams: None,
        });
        match admin {
            Ok(admin) => {
                let orgs = Arc::new(metadata::organizations::ProviderImpl::new(rocks.clone()));

                let org = orgs.create(CreateOrganizationRequest {
                    created_by: admin.id,
                    name: "Test Organization".to_string(),
                });

                match org {
                    Ok(org) => {
                        let projects =
                            Arc::new(metadata::projects::ProviderImpl::new(rocks.clone()));
                        _ = projects.create(org.id, CreateProjectRequest {
                            created_by: admin.id,
                            name: "Test Project".to_string(),
                        });
                    }
                    Err(_) => {}
                }
            }
            Err(_) => {}
        }
    }

    let event_props = Arc::new(properties::ProviderImpl::new_event(rocks.clone()));
    let events = Arc::new(events::ProviderImpl::new(rocks.clone()));
    let projects = Arc::new(projects::ProviderImpl::new(rocks.clone()));
    let dicts = Arc::new(dictionaries::ProviderImpl::new(rocks.clone()));

    let proj = projects.get_by_id(1, 1)?;
    debug!("project token: {}", proj.token);
    let mut track_transformers = Vec::new();
    let ua_parser = UserAgentParser::from_file(File::open(args.ua_db_path.clone())?)
        .map_err(|e| IngesterError::Internal(e.to_string()))?;
    let ua = user_agent::track::UserAgent::try_new(user_props.clone(), ua_parser)?;
    track_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Track>>);

    // todo make common
    let city_rdr = maxminddb::Reader::open_readfile(args.geo_city_path.clone())?;
    let geo = geo::track::Geo::try_new(user_props.clone(), city_rdr)?;
    track_transformers.push(Arc::new(geo) as Arc<dyn Transformer<Track>>);

    let mut track_destinations = Vec::new();
    let track_local_dst = ingester::destinations::local::track::Local::new(
        Arc::new(db),
        dicts.clone(),
        event_props.clone(),
        user_props.clone(),
    );
    track_destinations.push(Arc::new(track_local_dst) as Arc<dyn Destination<Track>>);
    let track_exec = Executor::<Track>::new(
        track_transformers.clone(),
        track_destinations.clone(),
        event_props.clone(),
        user_props.clone(),
        events.clone(),
        projects.clone(),
        dicts.clone(),
    );

    let mut identify_transformers = Vec::new();
    let ua_parser = UserAgentParser::from_file(File::open(args.ua_db_path.clone())?)
        .map_err(|e| IngesterError::Internal(e.to_string()))?;

    let ua = user_agent::identify::UserAgent::try_new(user_props.clone(), ua_parser)?;
    identify_transformers.push(Arc::new(ua) as Arc<dyn Transformer<Identify>>);

    let city_rdr = maxminddb::Reader::open_readfile(args.geo_city_path.clone())?;
    let geo = geo::identify::Geo::try_new(user_props.clone(), city_rdr)?;
    identify_transformers.push(Arc::new(geo) as Arc<dyn Transformer<Identify>>);

    let mut identify_destinations = Vec::new();
    let identify_debug_dst = ingester::destinations::debug::identify::Debug::new();
    identify_destinations.push(Arc::new(identify_debug_dst) as Arc<dyn Destination<Identify>>);
    let identify_exec = Executor::<Identify>::new(
        identify_transformers,
        identify_destinations,
        event_props.clone(),
        user_props.clone(),
        events.clone(),
        projects.clone(),
        dicts.clone(),
    );
    let svc = sources::http::service::Service::new(track_exec, identify_exec, args.host);

    info!("start listening on {}", args.host);

    Ok(svc.serve().await?)
}