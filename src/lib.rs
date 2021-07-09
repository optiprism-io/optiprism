#![allow(warnings)]
// mod operators;
mod arrow;
mod csv;
pub mod expression_tree;
pub mod physical_plan;
mod str;
mod error;

use datafusion::error::Result;
use std::any::Any;
#[macro_use] extern crate enum_dispatch;
struct V(i8);
#[tokio::main]
async fn main() -> Result<()> {
    let a = &V(0);
    let b = a as &dyn Any;

    Ok(())
}