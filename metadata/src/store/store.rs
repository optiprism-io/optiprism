use crate::Result;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use std::str::FromStr;


/*#[derive(Clone)]
enum Entity {
    Events,
    CustomEvents,
    EventProperties,
    EventCustomProperties,
    Accounts,
}

#[derive(Clone)]
enum Sequence {
    EntityPrimaryKeys(Entity),
    EventPropertyColumns,
}

#[derive(Clone)]
enum Namespace {
    Sequence(Sequence),
    General,
    Entity(Entity),
    EntityPrimaryKeySequences(Entity),
    EntitySecondaryIndices(Entity),
}

impl Namespace {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Namespace::General => "general",
            Namespace::Entity(Entity::Events) => "entities:events",
            Namespace::Entity(Entity::CustomEvents) => "entities:custom_events",
            Namespace::Entity(Entity::EventProperties) => "entities:event_properties",
            Namespace::Entity(Entity::EventCustomProperties) => "entities:event_custom_properties",
            Namespace::EntitySecondaryIndices(Entity::Events) => "entities_idx:events",
            Namespace::EntitySecondaryIndices(Entity::CustomEvents) => "entities_idx:custom_events",
            Namespace::EntitySecondaryIndices(Entity::EventProperties) => "entities_idx:event_properties",
            Namespace::EntitySecondaryIndices(Entity::EventCustomProperties) => "entities_idx:event_custom_properties",
            Namespace::EntitySecondaryIndices(Entity::Accounts) => "entity_idx:accounts",
            Namespace::Sequence(seq) => match seq {
                Sequence::EntityPrimaryKeys(e) => match e {
                    Entity::Events => ""
                    Entity::CustomEvents => {}
                    Entity::EventProperties => {}
                    Entity::EventCustomProperties => {}
                    Entity::Accounts => {}
                }
                Sequence::EventPropertyColumns => {}
            }
            Namespace::EntityPrimaryKeySequences(_) => {}
        }.as_bytes()
    }
}
*/
type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct Store {
    db: DB,
}

fn make_key(ns: &[u8], key: &[u8]) -> Vec<u8> {
    [ns, b":", key].concat()
}

enum ColumnFamily {
    General,
}

fn cf_descriptor(cf: ColumnFamily) -> ColumnFamilyDescriptor {
    match cf {
        ColumnFamily::General => ColumnFamilyDescriptor::new("general", Options::default()),
    }
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Store {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let cf_descriptors = vec![
            cf_descriptor(ColumnFamily::General),
        ];

        let db = DB::open_cf_descriptors(&options, path, cf_descriptors).unwrap();
        Store {
            db,
        }
    }

    fn cf_handle(&self, cf: ColumnFamily) -> Arc<BoundColumnFamily> {
        match cf {
            ColumnFamily::General => self.db.cf_handle("general").unwrap(),
        }
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<()>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        Ok(self.db.put(key.as_ref(), value.as_ref())?)
    }

    pub async fn put_checked<K, V>(
        &self,
        key: K,
        value: V,
    ) -> Result<Option<Vec<u8>>>
        where
            K: AsRef<[u8]> + Clone,
            V: AsRef<[u8]>,
    {
        match self.db.get(key.as_ref())? {
            None => Ok(None),
            Some(v) => {
                self.db.put(key.as_ref(), value)?;
                Ok(Some(v))
            }
        }
    }

    pub async fn multi_put(&self, kv: Vec<KVBytes>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (k, v) in kv.iter() {
            batch.put(k, v);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn get<K>(&self, key: K) -> Result<Option<Vec<u8>>>
        where
            K: AsRef<[u8]>,
    {
        Ok(self.db.get(key.as_ref())?)
    }

    pub async fn multi_get(&self, keys: Vec<&[u8]>) -> Result<Vec<Option<Vec<u8>>>> {
        Ok(keys
            .iter()
            .map(|key| self.db.get(key))
            .collect::<std::result::Result<_, _>>()?)
    }

    pub async fn delete<K>(&self, key: K) -> Result<()>
        where
            K: AsRef<[u8]> + Clone,
    {
        Ok(self.db.delete(key.as_ref())?)
    }

    pub async fn delete_checked<K>(&self, key: K) -> Result<Option<Vec<u8>>>
        where
            K: AsRef<[u8]> + Clone,
    {
        match self.db.get(key.as_ref())? {
            None => Ok(None),
            Some(v) => {
                self.db.delete(key.as_ref())?;
                Ok(Some(v))
            }
        }
    }

    pub async fn multi_delete(&self, keys: Vec<&[u8]>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for key in keys.iter() {
            batch.delete(key);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn list_prefix(&self, prefix: &[u8]) -> Result<Vec<KVBytes>> {
        let iter = self
            .db
            .prefix_iterator(prefix);
        Ok(iter.map(|v| (v.0.clone(), v.1.clone())).collect())
    }

    pub async fn next_seq(&self, key: &[u8]) -> Result<u64> {
        let id = self.db.get(key)?;
        let result: u64 = match id {
            Some(v) => u64::from_le_bytes(v.try_into()?) + 1,
            None => 1,
        };
        self.db.put(key, result.to_le_bytes())?;
        Ok(result)
    }
}
