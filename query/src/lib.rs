pub mod context;
pub mod data_table;
pub mod error;
pub mod expr;
pub mod logical_plan;
pub mod physical_plan;
pub mod provider;
pub mod queries;
pub mod test_util;

pub use context::Context;
pub use error::{Error, Result};
pub use provider::QueryProvider;

pub mod event_fields {
    pub const EVENT: &str = "event_event";
    pub const CREATED_AT: &str = "event_created_at";
    pub const USER_ID: &str = "event_user_id";
}

pub const DEFAULT_BATCH_SIZE: usize = 4096;
