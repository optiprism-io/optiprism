#![allow(warnings)]

use std::any::Any;

use datafusion::error::Result;

mod csv;
mod error;
mod execution;
mod ifaces;
mod logical_plan;
mod physical_plan;
pub mod segment;
mod str;
pub mod utils;