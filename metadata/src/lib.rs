pub mod accounts;
pub mod error;
pub mod events;
pub mod metadata;
pub mod organizations;
pub mod projects;
pub mod store;
pub mod event_properties;
mod column;

use arrow::datatypes::Schema;
use datafusion::scalar::ScalarValue;
pub use error::{Error, Result};
pub use metadata::Metadata;
pub use store::store::Store;

trait ColumnWriters {
    fn values(&self, schema: &Schema) -> Vec<ScalarValue>;
}