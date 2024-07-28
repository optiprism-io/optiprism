use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use metrics::Level;
use tracing::info;
use tracing_subscriber::FmtSubscriber;
use cmd::config::{Config, LogLevel};
use cmd::store::Store;
use cmd::test::Test;

extern crate parse_duration;

use cmd::error::Error;
use cmd::error::Result;
use cmd::test;

#[derive(Parser, Clone)]
pub struct Cfg {
    #[arg(long)]
    config: PathBuf,
}

#[derive(Subcommand, Clone)]
enum Commands {
    /// Run server
    Server(Cfg),
    /// Run demo store
    Store(Store),
    /// Run test suite
    Test(Test),
}

#[derive(Parser)]
#[command(propagate_version = true)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    if args.command.is_none() {
        return Err(Error::BadRequest("no command specified".to_string()));
    }

    let cfg: Config = match &args.command {
        Some(cmd) => match cmd {
            Commands::Server(cfg) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(cfg.config.clone()))
                    .build()?;
                config.try_deserialize()?
            }
            Commands::Store(store) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(store.config.clone()))
                    .build()?;

                config.try_deserialize()?
            }
            Commands::Test(args) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(args.config.clone()))
                    .build()?;

                config.try_deserialize()?
            }
        },
        _ => unreachable!(),
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(cfg.log.level)
        .finish();
    tracing::subscriber::set_global_default(subscriber).map_err(|e| Error::SetGlobalDefaultError(e))?;

    let version = env!("CARGO_PKG_VERSION");
    let hash = option_env!("BUILD_HASH").unwrap_or("dev-build");

    info!("Optiprism v{version}-{hash}");

    match &args.command {
        Some(cmd) => match cmd {
            Commands::Server(_) => {
                cmd::server::start(cfg.try_into()?).await?;
            }
            Commands::Store(store) => {
                cmd::store::start(store, cfg.try_into()?).await?;
            }
            Commands::Test(args) => {
                test::gen(args, cfg.try_into()?).await?;
            }
        },
        _ => unreachable!(),
    };

    Ok(())
}