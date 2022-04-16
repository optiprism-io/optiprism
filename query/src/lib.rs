pub mod context;
pub mod error;
pub mod logical_plan;
pub mod physical_plan;
pub mod provider;
pub mod reports;

pub use context::Context;
pub use error::{Error, Result};
pub use provider::Provider as QueryProvider;

pub mod event_fields {
    pub const EVENT: &str = "event";
    pub const CREATED_AT: &str = "created_at";
    pub const USER_ID: &str = "user_id";
}
