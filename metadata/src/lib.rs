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

pub use crate::metadata::Metadata;
pub use error::{Error, Result};

type OptionalProperty<T> = Option<T>;
