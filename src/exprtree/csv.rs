use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::DataType::Utf8;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::datasource::CsvReadOptions;
use datafusion::error::Result;
use datafusion::prelude::ExecutionContext;
use std::ops::Deref;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    ctx.register_csv("users", "tests/users.csv", CsvReadOptions::new())?;

    ctx.register_csv("events", "tests/events.csv", CsvReadOptions::new())?;

    let df = ctx.sql("e.name from users as u left join events as e on u.id=e.id")?;
    let results = df.collect().await?;
    pretty::print_batches(&results)?;
    Ok(())
}
