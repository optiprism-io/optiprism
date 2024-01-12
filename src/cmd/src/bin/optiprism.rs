use clap::Parser;
use clap::Subcommand;
use cmd::store::Shop;
use cmd::test::Test;
use service::tracing::TracingCliArgs;

extern crate parse_duration;

use cmd::error::Error;
use cmd::error::Result;
use cmd::test;

#[derive(Subcommand, Clone)]
enum Commands {
    /// Run demo shop
    Shop(Shop),
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
            Commands::Shop(shop) => {
                cmd::store::start(shop, 1).await?;
            }
            Commands::Test(args) => {
                test::gen(args, 1).await?;
            }
        },
        _ => unreachable!(),
    };

    Ok(())
}
