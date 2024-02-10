
use std::path::PathBuf;

use clap::Parser;
use clap::Subcommand;
use cmd::store::Store;
use cmd::test::Test;
use service::tracing::TracingCliArgs;

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
    #[clap(flatten)]
    tracing: TracingCliArgs,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    args.tracing.init()?;

    if args.command.is_none() {
        return Err(Error::BadRequest("no command specified".to_string()));
    }

    match &args.command {
        Some(cmd) => match cmd {
            Commands::Server(cfg) => {
                let config = config::Config::builder()
                    .add_source(config::File::from(cfg.config.clone()))
                    .build()?;

                let cfg: server::Config = config.try_deserialize()?;
                cmd::server::start(cfg).await?;
            }
            Commands::Store(store) => {
                cmd::store::start(store).await?;
            }
            Commands::Test(args) => {
                test::gen(args).await?;
            }
        },
        _ => unreachable!(),
    };

    Ok(())
}
