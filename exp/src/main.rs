#![allow(warnings)]

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::prelude::*;
use std::env;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // register the table
    let mut ctx = ExecutionContext::new();
    ctx.register_csv(
        "events",
        "/Users/ravlio/work/rust/exprtree/tests/events.csv",
        CsvReadOptions::new(),
    )
    .await?;
    // create a plan to run a SQL query
    // events total count
    // SELECT event, count(*) FROM events where event='buy' group by event
    // unique users
    // SELECT event, count_distinct_sorted(user) FROM events where event='buy' group by event
    // total per user
    // SELECT event, count_sorted_per(user) FROM events where event='buy' group by event
    // total per user
    // SELECT event, count_sorted_per(user) FROM events where event='buy' group by event

    let df = ctx.sql("SELECT * FROM events where i = null").await?;

    // execute and print results
    df.show().await?;
    Ok(())
}
