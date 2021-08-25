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
use datafusion::logical_plan::{JoinType, col, count, lit, and, case, when};

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

    let events = ctx.read_csv("tests/events.csv", CsvReadOptions::new())?
        .filter(col("prop").eq(lit("rock")))?;
    let users = ctx.read_csv("tests/users.csv", CsvReadOptions::new())?
        .filter(col("country").eq(lit("uk")).or(col("country").eq(lit("us"))))?;
    let df = users
        .join(events, JoinType::Inner, &["id"], &["user_id"])?
        // .filter(and(col("country").eq(lit("uk")).or(col("country").eq(lit("us"))), col("prop").eq(lit("rock"))))?
        // .filter(col("user_id").eq(lit(1)))?
        .aggregate(vec![when(col("name").eq(lit("search")), lit("search")).end()?], vec![count(col("name"))])?;
        // .explain(false)?;
    let results = df.collect().await?;
    pretty::print_batches(&results)?;
    Ok(())
}