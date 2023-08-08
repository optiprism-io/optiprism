// Привет. На самом деле всё же будет нужно гибридное (колоночное+kv) решение для хранения юзеров, 
// так как я нашёл один запрос с джойном. Но давай это отложим и вместо этого лучше спроектируем сущности БД так, чтобы потом можно было всё расширить.
//
// - Возможность создавать один или более стораджа. Сторадж закрепляется за таблицой. 
//   В нашем случае, SortrdMergeTree зауреплен за таблицей events. Но никто не мешает потом добавить ещё таблиц с этим стораджом или другим (например, ReplacingSortedMergeTree)
// - В очень недалёком будущем — Materialized view (async и sync). Возможность делать агрегации таблицы. Например, сортировку по другим колонкам, 
//   или агрегации данных (вот тут тоже будет любопытно, тк будет уже связь с с движком запросов)
// - Миграции из таблицы в таблицу (а-ля insert into ... select from). Синхронная, асинхронна. По сути Materialized View.
// 
// У нас не будет богатого недетерминированного DDL, как в SQL-db, скорее всё будет управляться кодом с возможностью конфигурирования.


use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{RwLock, Arc};

mod engine;
mod ids;
mod table;
mod wal;
mod engines_impl;

use self::engine::OptiEngine;

pub struct OptiStorage
{
    path: PathBuf,
    // TODO: switch to lock-free hashmap
    tables: Arc<RwLock<HashMap<String, Box<dyn OptiEngine>>>>,
}

impl OptiStorage {
    fn create(path: &str) -> Result<Self, String> {
        let path = PathBuf::from(path);
        let tables = Arc::new(RwLock::new(HashMap::new()));
        Ok(Self { path, tables })
    }
}

#[cfg(test)]
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
