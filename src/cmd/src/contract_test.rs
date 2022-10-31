use std::net::SocketAddr;

use clap::Args;

use crate::Result;

#[derive(Args)]
pub struct Config {
    #[arg(long, default_value = "0.0.0.0:8080")]
    host: SocketAddr,
}

pub async fn run(cfg: Config) -> Result<()> {
    Ok(contract_test::run(cfg.host).await?)
}
