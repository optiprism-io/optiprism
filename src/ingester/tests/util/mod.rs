//! Test utilities

use std::env::temp_dir;
use std::sync::Arc;

use metadata::store::Store;
use uuid::Uuid;

pub fn tmp_store() -> Arc<Store> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    Arc::new(Store::new(path))
}
