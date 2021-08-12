#![allow(warnings)]

use std::any::Any;

use datafusion::error::Result;

mod csv;
pub mod segment;
mod str;
mod error;
pub mod utils;
mod logical_plan;
mod physical_plan;
mod execution;
mod ifaces;