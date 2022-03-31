mod api;
pub mod common;
pub mod context;
pub mod error;
pub mod event_segmentation;
pub mod logical_plan;
pub mod physical_plan;

pub use context::Context;
pub use error::{Error, Result};

pub mod event_fields {
    pub const EVENT: &str = "event";
    pub const CREATED_AT: &str = "created_at";
    pub const USER_ID: &str = "user_id";
}
