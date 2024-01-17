use std::fs;
use std::fs::File;
use std::path::PathBuf;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::print;
use scan_dir::ScanDir;

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
#[allow(clippy::type_complexity)]
fn read_chunks(path: PathBuf) {
    let mut file = File::open(path).unwrap();

    // read the files' metadata. At this point, we can distribute the read whatever we like.
    let metadata = read::read_file_metadata(&mut file).unwrap();

    let schema = metadata.schema.clone();

    // Simplest way: use the reader, an iterator over batches.
    let reader = read::FileReader::new(file, metadata, None, None);

    for chunk in reader.into_iter() {}
}

#[test]
fn test_parquet() {
    read_chunks(PathBuf::from("/tmp/1.parquet"));
}
