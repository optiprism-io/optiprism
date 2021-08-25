use std::sync::Arc;
use arrow::array::{Int32Array, StringArray, ArrayRef};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::ops::Deref;
use arrow::datatypes::DataType::Utf8;
use datafusion::prelude::ExecutionContext;
use datafusion::datasource::CsvReadOptions;
use arrow::util::pretty;
use datafusion::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut ctx = ExecutionContext::new();

    ctx.register_csv(
        "users",
        "tests/users.csv",
        CsvReadOptions::new(),
    )?;

    ctx.register_csv(
        "events",
        "tests/events.csv",
        CsvReadOptions::new(),
    )?;

    let df = ctx.sql("e.name from users as u left join events as e on u.id=e.id")?;
    let results = df.collect().await?;
    pretty::print_batches(&results)?;
    Ok(())
}