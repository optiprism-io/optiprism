pub mod accounts;
pub mod database;
pub mod dictionaries;
pub mod error;
pub mod events;
pub mod metadata;
pub mod organizations;
pub mod projects;
pub mod properties;
pub mod store;
pub mod custom_events;
pub mod types;
pub mod scalar;

pub use crate::metadata::Metadata;
pub use error::{ Result};

type OptionalProperty<T> = Option<T>;
