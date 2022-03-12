mod api;
pub mod context;
pub mod error;
// pub mod event_segmentation;
pub mod logical_plan;
pub mod physical_plan;
mod result;

pub use context::Context;
pub use error::{Error, Result};
