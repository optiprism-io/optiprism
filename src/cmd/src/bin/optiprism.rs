use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use metrics::Level;
use tracing_subscriber::FmtSubscriber;
use cmd::config::{Config, LogLevel};
use cmd::store::Store;
use cmd::test::Test;

extern crate parse_duration;

use cmd::error::Error;
use cmd::error::Result;
use cmd::server;
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

    match &args.command {
        Some(cmd) => match cmd {
            Commands::Server(cfg) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(cfg.config.clone()))
                    .build()?;

                let cfg: Config = config.try_deserialize()?;

                let subscriber = FmtSubscriber::builder()
                    .with_max_level(cfg.log.level)
                    .finish();

                tracing::subscriber::set_global_default(subscriber).map_err(|e| Error::SetGlobalDefaultError(e))?;


                cmd::server::start(cfg.try_into()?).await?;
            }
            Commands::Store(store) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(store.config.clone()))
                    .build()?;

                let cfg: Config = config.try_deserialize()?;
                let subscriber = FmtSubscriber::builder()
                    .with_max_level(cfg.log.level)
                    .finish();

                tracing::subscriber::set_global_default(subscriber).map_err(|e| Error::SetGlobalDefaultError(e))?;

                cmd::store::start(store, cfg.try_into()?).await?;
            }
            Commands::Test(args) => {
                test::gen(args).await?;
            }
        },
        _ => unreachable!(),
    };

    Ok(())
}
