#![allow(warnings, unused)]

mod exprtree;
mod storage;
mod user_storage;

use actix_web::{get, web, App, HttpServer};
use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::DataType::Utf8;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::datasource::CsvReadOptions;
use datafusion::error::Result;
use datafusion::logical_plan::{and, case, col, count, lit, when, JoinType};
use datafusion::prelude::ExecutionContext;
use std::env::var;
use std::ops::Deref;
use std::sync::Arc;
#[get("/")]
async fn index() -> &'static str {
    "Hello, World!"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_csv("users", "tests/users.csv", CsvReadOptions::new())
        .unwrap();
    ctx.register_csv("events", "tests/events.csv", CsvReadOptions::new())
        .unwrap();
    let events = ctx
        .read_csv("tests/events.csv", CsvReadOptions::new())
        .unwrap()
        .filter(col("prop").eq(lit("rock")))
        .unwrap();
    let users = ctx
        .read_csv("tests/users.csv", CsvReadOptions::new())
        .unwrap()
        .filter(
            col("country")
                .eq(lit("uk"))
                .or(col("country").eq(lit("us"))),
        )
        .unwrap();
    let df = users
        .join(events, JoinType::Inner, &["id"], &["user_id"])
        .unwrap()
        // .filter(and(col("country").eq(lit("uk")).or(col("country").eq(lit("us"))), col("prop").eq(lit("rock"))))?
        // .filter(col("user_id").eq(lit(1)))?
        .aggregate(
            vec![when(col("name").eq(lit("search")), lit("search"))
                .end()
                .unwrap()],
            vec![count(col("name"))],
        )
        .unwrap();
    // .explain(false)?;
    let results = df.collect().await.unwrap();
    pretty::print_batches(&results).unwrap();
    Ok(())

    /*
    // let user_service = web::Data::new(services::user::Service::new(pool.clone()).await.unwrap());
    // let auth_service = web::Data::new(services::auth::Service::new(
    //     user_service.clone().into_inner(),
    // ));
    HttpServer::new(move || {
        App::new().service(index)
        // .app_data(user_service.clone())
        // .configure(services::user::endpoints)
        // .app_data(auth_service.clone())
        // .configure(services::auth::endpoints)
    })
    .bind(var("ET_BIND_ADDRESS").unwrap())?
    .run()
    .await
    */
}
