#![allow(warnings)]

use std::any::Any;

use datafusion::error::Result;

// mod operators;
mod csv;
pub mod segment;
mod str;
mod error;
pub mod utils;
mod logical_plan;
mod physical_plan;
mod execution;

#[macro_use] extern crate enum_dispatch;
struct V(i8);
#[tokio::main]
async fn main() -> Result<()> {
    let a = &V(0);
    let b = a as &dyn Any;

    Ok(())
}
