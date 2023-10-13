//! Ingester error definitions.

use metadata::error::MetadataError;

/// Ingester errors
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("metadata error: {0}")]
    Metadata(#[from] MetadataError),
}
