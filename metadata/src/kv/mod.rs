use std::path::Path;
use std::sync::{Arc, RwLock};
use crate::error::Result;
use chrono::{DateTime, Utc};
use datafusion::parquet::data_type::AsBytes;
use rocksdb::{DB, ColumnFamilyDescriptor, Options, DBCompressionType, BlockBasedOptions, BoundColumnFamily, ColumnFamily, IteratorMode, WriteBatch};
use serde::__private::de::IdentifierDeserializer;

pub struct Meta {
    expired_at: DateTime<Utc>,
}

#[derive(Clone)]
pub enum Table {
    // table for non-entities
    Sequences = 0,
    General = 1,
    Events = 2,
    CustomEvents = 3,
    EventProperties = 4,
    EventCustomProperties = 5,
}

impl Table {
    fn to_bytes(&self) -> &[u8] {
        let i = self.clone() as usize;
        i.to_le_bytes().as_ref()
    }
}

static CF_NAME_SEQUENCES: &str = "sequences";
static CF_NAME_GENERAL: &str = "general";
static CF_NAME_EVENTS: &str = "events";
static CF_CUSTOM_EVENTS: &str = "custom_events";
static CF_EVENT_PROPERTIES: &str = "event_properties";
static CF_EVENT_CUSTOM_PROPERTIES: &str = "event_custom_properties";

fn cf_descriptor(t: Table) -> ColumnFamilyDescriptor {
    match t {
        Table::Sequences => {
            ColumnFamilyDescriptor::new(CF_NAME_SEQUENCES, Options::default())
        }
        Table::General => {
            ColumnFamilyDescriptor::new(CF_NAME_GENERAL, Options::default())
        }
        Table::Events => {
            ColumnFamilyDescriptor::new(CF_NAME_EVENTS, Options::default())
        }
        Table::CustomEvents => {
            ColumnFamilyDescriptor::new(CF_CUSTOM_EVENTS, Options::default())
        }
        Table::EventProperties => {
            ColumnFamilyDescriptor::new(CF_EVENT_PROPERTIES, Options::default())
        }
        Table::EventCustomProperties => {
            ColumnFamilyDescriptor::new(CF_EVENT_CUSTOM_PROPERTIES, Options::default())
        }
    }
}

type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct KV {
    db: DB,
    seq_guard: Vec<RwLock<()>>,
}


impl KV {
    pub fn new<P: AsRef<Path>>(path: P) -> KV {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let db = DB::open_cf_descriptors(
            &options,
            path, vec![
                cf_descriptor(Table::Sequences),
                cf_descriptor(Table::General),
                cf_descriptor(Table::Events),
                cf_descriptor(Table::CustomEvents),
                cf_descriptor(Table::EventProperties),
                cf_descriptor(Table::EventCustomProperties),
            ]).unwrap();

        KV { db, seq_guard: (0..5).into_iter().map(|_| RwLock::new(())).collect() }
    }

    fn cf_handle(&self, t: Table) -> Arc<BoundColumnFamily> {
        match t {
            Table::Sequences => {
                self.db.cf_handle(CF_NAME_SEQUENCES).unwrap()
            }
            Table::General => {
                self.db.cf_handle(CF_NAME_GENERAL).unwrap()
            }
            Table::Events => {
                self.db.cf_handle(CF_NAME_EVENTS).unwrap()
            }
            Table::CustomEvents => {
                self.db.cf_handle(CF_CUSTOM_EVENTS).unwrap()
            }
            Table::EventProperties => {
                self.db.cf_handle(CF_EVENT_PROPERTIES).unwrap()
            }
            Table::EventCustomProperties => {
                self.db.cf_handle(CF_EVENT_CUSTOM_PROPERTIES).unwrap()
            }
        }
    }

    pub async fn put(&self, t: Table, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put_cf(
            &self.cf_handle(t),
            key,
            value,
        )?)
    }

    pub async fn multi_put(&self, t: Table, kv: Vec<KVBytes>) -> Result<()> {
        let cf = self.cf_handle(t);
        let mut batch = WriteBatch::default();
        for (k, v) in kv.iter() {
            batch.put_cf(&cf, k, v);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(
            &self.cf_handle(t),
            key,
        )?)
    }

    pub async fn multi_get(&self, t: Table, keys: Vec<&[u8]>) -> Result<Vec<Option<Vec<u8>>>> {
        let cf = self.cf_handle(t);
        Ok(keys.iter().map(|key| {
            self.db.get_cf(&cf, key)
        }).collect::<std::result::Result<_, _>>()?)
    }

    pub async fn delete(&self, t: Table, key: &[u8]) -> Result<()> {
        Ok(self.db.delete_cf(
            &self.cf_handle(t),
            key,
        )?)
    }

    pub async fn multi_delete(&self, t: Table, keys: Vec<&[u8]>) -> Result<()> {
        let cf = self.cf_handle(t);
        let mut batch = WriteBatch::default();
        for key in keys.iter() {
            batch.delete_cf(&cf, key);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn list(&self, t: Table) -> Result<Vec<KVBytes>> {
        let mut iter = self.db.iterator_cf(
            &self.cf_handle(t),
            IteratorMode::Start,
        );

        Ok(iter.map(|v| (v.0.clone(), v.1.clone())).collect())
    }

    pub async fn next_seq(&self, table: Table) -> Result<u64> {
        let _guard = self.seq_guard[table as usize].write();
        let cf = &self.cf_handle(Table::Sequences);
        let key = table.to_bytes();
        let id = self.db.get_cf(&cf, key)?;
        let result: u64 = match id {
            Some(v) => {
                u64::from_le_bytes(v.try_into()?) + 1
            }
            None => 1
        };

        self.db.put_cf(&cf, key, result.to_le_bytes())?;
        Ok(result)
    }

    pub async fn entity_insert(
        &self,
        table: Table,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<u64> {
        unimplemented!()
    }

    pub async fn entity_update(
        &self,
        table: Table,
        id: u64,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<u64> {
        unimplemented!()
    }

    pub async fn entity_put(
        &self,
        table: Table,
        id: u64,
        value: &[u8],
        meta: Option<Meta>,
    ) -> Result<()> {
        unimplemented!()
    }

    pub async fn entity_get(&self, table: Table, id: u64) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub async fn entity_multi_get(
        &self,
        table: Table,
        id: Vec<u64>,
    ) -> Result<Vec<Option<Vec<u8>>>> {
        unimplemented!()
    }

    pub async fn entity_delete(&self, table: Table, id: u64) -> Result<()> {
        unimplemented!()
    }
}
