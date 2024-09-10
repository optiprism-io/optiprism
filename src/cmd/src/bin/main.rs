use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use cmd::command::db_test;
use cmd::command::db_test::DbTest;
use cmd::command::server;
use cmd::command::store;
use cmd::command::store::Store;
use cmd::command::test;
use cmd::command::test::Test;
use cmd::config::Config;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

extern crate parse_duration;

use cmd::error::Error;
use cmd::error::Result;

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
    DbTest(DbTest),
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
            Commands::DbTest(args) => {
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
    tracing::subscriber::set_global_default(subscriber).map_err(Error::SetGlobalDefaultError)?;

    let version = env!("CARGO_PKG_VERSION");
    let hash = option_env!("BUILD_HASH").unwrap_or("dev-build");

    info!("Optiprism v{version}-{hash}");

    match &args.command {
        Some(cmd) => match cmd {
            Commands::Server(_) => {
                server::start(cfg.try_into()?).await?;
            }
            Commands::Store(store) => {
                store::start(store, cfg.try_into()?).await?;
            }
            Commands::Test(args) => {
                test::gen(args, cfg.try_into()?).await?;
            }
            Commands::DbTest(args) => match &args.cmd {
                db_test::Commands::Gen(gen) => {
                    db_test::gen(args, gen, cfg.try_into()?).await?;
                }
                db_test::Commands::Query(query) => {
                    db_test::query(args, query).await?;
                }
            },
        },
        _ => unreachable!(),
    };

    Ok(())
}
