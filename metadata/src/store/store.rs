use crate::Result;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub enum Namespace {
    Sequences = 0,
    // namespace for non-entities
    General = 1,
    Events = 2,
    CustomEvents = 3,
    EventProperties = 4,
    EventCustomProperties = 5,
    Accounts = 6,
}

impl Namespace {
    fn to_bytes(&self) -> [u8; 1] {
        [self.clone() as u8]
    }
}

static CF_NAME_SEQUENCES: &str = "sequences";
static CF_NAME_GENERAL: &str = "general";
static CF_NAME_EVENTS: &str = "events";
static CF_CUSTOM_EVENTS: &str = "custom_events";
static CF_EVENT_PROPERTIES: &str = "event_properties";
static CF_EVENT_CUSTOM_PROPERTIES: &str = "event_custom_properties";
static CF_ACCOUNTS: &str = "accounts";

fn cf_descriptor(t: Namespace) -> ColumnFamilyDescriptor {
    match t {
        Namespace::Sequences => ColumnFamilyDescriptor::new(CF_NAME_SEQUENCES, Options::default()),
        Namespace::General => ColumnFamilyDescriptor::new(CF_NAME_GENERAL, Options::default()),
        Namespace::Events => ColumnFamilyDescriptor::new(CF_NAME_EVENTS, Options::default()),
        Namespace::CustomEvents => {
            ColumnFamilyDescriptor::new(CF_CUSTOM_EVENTS, Options::default())
        }
        Namespace::EventProperties => {
            ColumnFamilyDescriptor::new(CF_EVENT_PROPERTIES, Options::default())
        }
        Namespace::EventCustomProperties => {
            ColumnFamilyDescriptor::new(CF_EVENT_CUSTOM_PROPERTIES, Options::default())
        }
        Namespace::Accounts => ColumnFamilyDescriptor::new(CF_ACCOUNTS, Options::default()),
    }
}

type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct Store {
    db: DB,
    ns_guard: Vec<RwLock<()>>,
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Store {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let cf_descriptors = vec![
            cf_descriptor(Namespace::Sequences),
            cf_descriptor(Namespace::General),
            cf_descriptor(Namespace::Events),
            cf_descriptor(Namespace::CustomEvents),
            cf_descriptor(Namespace::EventProperties),
            cf_descriptor(Namespace::EventCustomProperties),
        ];

        let cfd_len = cf_descriptors.len();

        let db = DB::open_cf_descriptors(&options, path, cf_descriptors).unwrap();
        Store {
            db,
            ns_guard: (0..cfd_len).into_iter().map(|_| RwLock::new(())).collect(),
        }
    }

    fn cf_handle(&self, t: Namespace) -> Arc<BoundColumnFamily> {
        match t {
            Namespace::Sequences => self.db.cf_handle(CF_NAME_SEQUENCES).unwrap(),
            Namespace::General => self.db.cf_handle(CF_NAME_GENERAL).unwrap(),
            Namespace::Events => self.db.cf_handle(CF_NAME_EVENTS).unwrap(),
            Namespace::CustomEvents => self.db.cf_handle(CF_CUSTOM_EVENTS).unwrap(),
            Namespace::EventProperties => self.db.cf_handle(CF_EVENT_PROPERTIES).unwrap(),
            Namespace::EventCustomProperties => {
                self.db.cf_handle(CF_EVENT_CUSTOM_PROPERTIES).unwrap()
            }
            Namespace::Accounts => self.db.cf_handle(CF_ACCOUNTS).unwrap(),
        }
    }

    pub async fn put<K, V>(&self, ns: Namespace, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        Ok(self.db.put_cf(&self.cf_handle(ns), key, value)?)
    }

    pub async fn put_checked<K, V>(
        &self,
        ns: Namespace,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]> + Clone,
        V: AsRef<[u8]>,
    {
        let _guard = self.ns_guard[ns.clone() as usize].write();

        match self.db.get_cf(&self.cf_handle(ns.clone()), key.clone())? {
            None => Ok(None),
            Some(v) => {
                self.db.put_cf(&self.cf_handle(ns), key, value)?;
                Ok(Some(v))
            }
        }
    }

    pub async fn multi_put(&self, t: Namespace, kv: Vec<KVBytes>) -> Result<()> {
        let cf = self.cf_handle(t);
        let mut batch = WriteBatch::default();
        for (k, v) in kv.iter() {
            batch.put_cf(&cf, k, v);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn get<K>(&self, t: Namespace, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]>,
    {
        Ok(self.db.get_cf(&self.cf_handle(t), key)?)
    }

    pub async fn multi_get(&self, ns: Namespace, keys: Vec<&[u8]>) -> Result<Vec<Option<Vec<u8>>>> {
        let cf = self.cf_handle(ns);
        Ok(keys
            .iter()
            .map(|key| self.db.get_cf(&cf, key))
            .collect::<std::result::Result<_, _>>()?)
    }

    pub async fn delete<K>(&self, ns: Namespace, key: K) -> Result<()>
    where
        K: AsRef<[u8]> + Clone,
    {
        Ok(self.db.delete_cf(&self.cf_handle(ns), key)?)
    }

    pub async fn delete_checked<K>(&self, ns: Namespace, key: K) -> Result<Option<Vec<u8>>>
    where
        K: AsRef<[u8]> + Clone,
    {
        let _guard = self.ns_guard[ns.clone() as usize].write();

        match self.db.get_cf(&self.cf_handle(ns.clone()), key.clone())? {
            None => Ok(None),
            Some(v) => {
                self.db.delete_cf(&self.cf_handle(ns), key)?;
                Ok(Some(v))
            }
        }
    }

    pub async fn multi_delete(&self, ns: Namespace, keys: Vec<&[u8]>) -> Result<()> {
        let cf = self.cf_handle(ns);
        let mut batch = WriteBatch::default();
        for key in keys.iter() {
            batch.delete_cf(&cf, key);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn list(&self, ns: Namespace) -> Result<Vec<KVBytes>> {
        let iter = self
            .db
            .iterator_cf(&self.cf_handle(ns), IteratorMode::Start);
        Ok(iter.map(|v| (v.0.clone(), v.1.clone())).collect())
    }

    pub async fn next_seq(&self, ns: Namespace) -> Result<u64> {
        let _guard = self.ns_guard[ns.clone() as usize].write();
        let cf = self.cf_handle(Namespace::Sequences);
        let key = ns.to_bytes();
        let id = self.db.get_cf(&cf, key)?;
        let result: u64 = match id {
            Some(v) => u64::from_le_bytes(v.try_into()?) + 1,
            None => 1,
        };
        self.db.put_cf(&cf, key, result.to_le_bytes())?;
        Ok(result)
    }
}
