//! Events ingester module.

#[deny(missing_docs)]
mod http;
pub mod ingester;

pub use http::attach_routes;
