mod error;
pub mod event_segmentation;
pub mod logical_plan;
pub mod physical_plan;
mod api;
mod context;
mod result;

pub use context::Context;
pub use error::{Error, Result};
