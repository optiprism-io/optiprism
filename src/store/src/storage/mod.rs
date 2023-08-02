use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{RwLock, Arc};

mod engine;
mod ids;
mod table;

use table::OptiTable;

#[derive(Debug)]
pub struct OptiStorage {
    path: PathBuf,
    // TODO: switch to lock-free hashmap
    tables: Arc<RwLock<HashMap<String, OptiTable>>>,
}

#[test]
mod tests {
    use super::*;

    #[test]
    fn new_storage() {
        let path_to_storage = "/tmp/path_to_storage";
        assert_eq!(Ok(storage), OptiStorage::create(path_to_storage))
    }

    #[test]
    fn open_storage() {
        let path_to_storage = "/tmp/path_to_prepared_storage";
        assert_eq!(Ok(storage), OptiStorage::open(path_to_storage))
    }

    #[test]
    fn create_table() {
        let path_to_storage = "/tmp/path_to_storage";
        let storage = OptiStorage::create(path_to_storage);
        let table_cfg = TableCfg::new(name, schema, engine);
        assert_eq!(Ok(table), storage.create_table("table_name", table_cfg))
    }

    #[test]
    fn list_existed_tables() {
        let path_to_storage = "/tmp/path_to_prepared_storage";
        let storage = OptiStorage::open(path_to_storage);
        assert_eq!(storage.list_tables().len(), 1)
    }

    #[test]
    fn list_zero_tables() {
        let path_to_storage = "/tmp/path_to_storage";
        let storage = OptiStorage::create(path_to_storage);
        assert_eq!(storage.list_tables().len(), 0)
    }

    #[test]
    fn close_storage() {
        let path_to_storage = "/tmp/path_to_storage";
        let storage = OptiStorage::create(path_to_storage);
        assert_eq!(storage.close(), Ok(()))
    }

    #[test]
    fn close_storage_negative() {
        // actually this test for checking correctness of graceful shutdown
        todo!("check that storage can't be closed in case of existence ongoing operations ")
    }
}
