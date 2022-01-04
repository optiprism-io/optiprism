pub mod provider;
pub mod types;

pub use provider::Provider;
pub use types::Event;

pub const BASE_NAME: &str = "event";
pub const INDEX_NAME: &str = "event_index";
