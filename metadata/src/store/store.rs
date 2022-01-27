use crate::Result;
use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform, WriteBatch, DB};
use std::path::Path;

fn make_main_key(organization_id: u64, project_id: u64, ns: &[u8]) -> Vec<u8> {
    [
        b"organizations/",
        organization_id.to_le_bytes().as_ref(),
        b"projects/",
        project_id.to_le_bytes().as_ref(),
        b"/",
        ns,
        b"/",
    ].concat()
}

pub fn make_data_key(organization_id: u64, project_id: u64, ns: &[u8], id: u64) -> Vec<u8> {
    [
        make_main_key(organization_id, project_id, ns).as_slice(),
        b"data/",
        id.to_le_bytes().as_ref(),
    ]
        .concat()
}

pub fn make_index_key(organization_id: u64, project_id: u64, ns: &[u8], idx_name: &[u8], key: &str) -> Vec<u8> {
    [
        make_main_key(organization_id, project_id, ns).as_slice(),
        b"idx/",
        idx_name,
        b"/",
        key.as_bytes(),
    ]
        .concat()
}

pub fn make_id_seq_key(organization_id: u64, project_id: u64, ns: &[u8]) -> Vec<u8> {
    [
        make_main_key(organization_id, project_id, ns).as_slice(),
        b"seq/id",
    ]
        .concat()
}

type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct Store {
    db: DB,
}

enum ColumnFamily {
    General,
}

fn cf_descriptor(cf: ColumnFamily, opts: Options) -> ColumnFamilyDescriptor {
    match cf {
        ColumnFamily::General => ColumnFamilyDescriptor::new("general", opts),
    }
}

fn first_three(k: &[u8]) -> &[u8] {
    println!("cc {}", std::str::from_utf8(&k[..15]).unwrap());
    &k[..15]
}

impl Store {
    pub fn new<P: AsRef<Path>>(path: P) -> Store {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // TODO manage how to properly work with prefixes
        let prefix_extractor = SliceTransform::create("first_three", first_three, None);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(10));
        opts.create_missing_column_families(true);
        opts.set_prefix_extractor(prefix_extractor);

        let cf_descriptors = vec![cf_descriptor(ColumnFamily::General, opts.clone())];

        let db = DB::open_cf_descriptors(&opts, path, cf_descriptors).unwrap();
        Store { db }
    }

    pub async fn put<K, V>(&self, key: K, value: V) -> Result<()>
        where
            K: AsRef<[u8]>,
            V: AsRef<[u8]>,
    {
        Ok(self.db.put(key, value)?)
    }

    pub async fn put_checked<K, V>(&self, key: K, value: V) -> Result<Option<Vec<u8>>>
        where
            K: AsRef<[u8]> + Clone,
            V: AsRef<[u8]>,
    {
        match self.db.get(key.as_ref())? {
            None => Ok(None),
            Some(v) => {
                self.db.put(key, value)?;
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
        Ok(self.db.get(key)?)
    }

    pub async fn multi_get<K: AsRef<[u8]>>(&self, keys: Vec<&K>) -> Result<Vec<Option<Vec<u8>>>> {
        Ok(keys
            .iter()
            .map(|key| self.db.get(key))
            .collect::<std::result::Result<_, _>>()?)
    }

    pub async fn delete<K>(&self, key: K) -> Result<()>
        where
            K: AsRef<[u8]> + Clone,
    {
        Ok(self.db.delete(key)?)
    }

    pub async fn delete_checked<K>(&self, key: K) -> Result<Option<Vec<u8>>>
        where
            K: AsRef<[u8]> + Clone,
    {
        match self.db.get(key.as_ref())? {
            None => Ok(None),
            Some(v) => {
                self.db.delete(key)?;
                Ok(Some(v))
            }
        }
    }

    pub async fn multi_delete<K: AsRef<[u8]>>(&self, keys: Vec<&K>) -> Result<()> {
        let mut batch = WriteBatch::default();
        for key in keys.iter() {
            batch.delete(key);
        }
        Ok(self.db.write(batch)?)
    }

    pub async fn list_prefix<K: AsRef<[u8]>>(&self, prefix: K) -> Result<Vec<KVBytes>> {
        let prefix = prefix.as_ref();
        let iter = self.db.prefix_iterator(prefix);
        Ok(iter
            .filter_map(|v| {
                if v.0.len() > prefix.len() && v.0[..prefix.len()].eq(prefix) {
                    Some((v.0.clone(), v.1))
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn next_seq<K: AsRef<[u8]>>(&self, key: K) -> Result<u64> {
        let id = self.db.get(key.as_ref())?;
        let result: u64 = match id {
            Some(v) => u64::from_le_bytes(v.try_into()?) + 1,
            None => 1,
        };
        self.db.put(key, result.to_le_bytes())?;
        Ok(result)
    }
}

// https://github.com/paritytech/parity-common/blob/d8c63201624d39525198ce71fc550dd09a267271/kvdb/src/lib.rs#L155-L170
pub fn end_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end_range = prefix.to_vec();
    while let Some(0xff) = end_range.last() {
        end_range.pop();
    }
    if let Some(byte) = end_range.last_mut() {
        *byte += 1;
        Some(end_range)
    } else {
        None
    }
}
