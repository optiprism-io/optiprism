//! Events ingester module.

#[deny(missing_docs)]
mod http;
mod ingester;

pub use http::attach_routes;
