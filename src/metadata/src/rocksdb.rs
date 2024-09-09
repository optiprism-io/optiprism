use std::path::Path;

use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Options;
use rocksdb::TransactionDB;
use rocksdb::TransactionDBOptions;

use crate::Result;

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
    opts.create_missing_column_families(true);

    let cf_descriptors = vec![cf_descriptor(ColumnFamily::General, opts.clone())];
    let txopts = TransactionDBOptions::default();

    Ok(TransactionDB::open_cf_descriptors(
        &opts,
        &txopts,
        path,
        cf_descriptors,
    )?)
}
