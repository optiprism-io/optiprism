use std::path::Path;

use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Options;
use rocksdb::TransactionDB;
use rocksdb::TransactionDBOptions;
use rocksdb::WriteBatch;
use rocksdb::DB;

use crate::Result;

type KVBytes = (Box<[u8]>, Box<[u8]>);

pub struct Store {
    db: TransactionDB,
}

enum ColumnFamily {
    General,
}

fn cf_descriptor(cf: ColumnFamily, opts: Options) -> ColumnFamilyDescriptor {
    match cf {
        ColumnFamily::General => ColumnFamilyDescriptor::new("general", opts),
    }
}

pub fn new<P: AsRef<Path>>(path: P) -> Result<TransactionDB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    // TODO manage how to properly work with prefixes
    // let prefix_extractor = SliceTransform::create("first_three", first_three, None);
    // opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(10));
    opts.create_missing_column_families(true);
    // opts.set_prefix_extractor(prefix_extractor);

    let cf_descriptors = vec![cf_descriptor(ColumnFamily::General, opts.clone())];

    let txopts = TransactionDBOptions::default();
    Ok(TransactionDB::open_cf_descriptors(
        &opts,
        &txopts,
        path,
        cf_descriptors,
    )?)
}
