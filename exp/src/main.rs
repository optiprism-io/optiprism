#![allow(warnings)]

use std::env;
use datafusion::prelude::*;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // register the table
    println!("{}", env::current_dir()?.display());
    let mut ctx = ExecutionContext::new();
    ctx.register_csv("events", "/Users/ravlio/work/rust/exprtree/tests/events.csv", CsvReadOptions::new()).await?;
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