use std::collections::BinaryHeap;
use std::env::temp_dir;
use std::ffi::OsStr;
use std::fmt::Write;
use std::fs;
use std::fs::File;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use arrow::array::{ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array};
use arrow::array::StringBuilder;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytesize::ByteSize;
use chrono::Duration;
use chrono::Utc;
use clap::Parser;
use clap::Subcommand;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use dateparser::DateTimeUtc;
use futures::executor::block_on;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use scan_dir::ScanDir;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use platform::auth;
use platform::auth::password::make_password_hash;
use query::ProviderImpl;
use service::tracing::TracingCliArgs;
use tracing::debug;
use tracing::info;
use uuid::Uuid;
use cmd::store::Shop;
use cmd::test::Test;
use query::datasources::local::LocalTable;

extern crate parse_duration;

use cmd::error::{Error, Result};
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
        return Err(Error::BadRequest("no command specified".to_string()).into());
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

