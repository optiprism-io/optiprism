use std::fs::File;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[allow(clippy::type_complexity)]
fn read_chunks(path: PathBuf) {
    println!("{:?}", path);
    let file = File::open(path).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("{}", builder.metadata().num_row_groups());
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();
    let mut c = 0;
    loop {
        let a = reader.next();
        if let Some(_b) = a {
        } else {
            break;
        }
    }
    for b in reader.next() {
        println!("!");
        let bb = b.unwrap();
        c += bb.num_rows();
    }
    println!("Read {} records.", c);
}

fn main() {
    read_chunks(PathBuf::from(
        "/Users/maximbogdanov/user_files/store/tables/events/0/0/5.parquet",
    ));
    return;
    let ids = Int32Array::from(vec![1]);
    let vals = Int32Array::from(vec![1]);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(ids) as ArrayRef),
        ("val", Arc::new(vals) as ArrayRef),
    ])
    .unwrap();
    let schema = batch.schema();
    let w = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open("/tmp/1.parquet")
        .unwrap();
    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(w, schema.clone(), Some(props)).unwrap();
    for i in 0..100 {
        println!("{i}");
        let ids = Int32Array::from((1..1000000).into_iter().collect::<Vec<i32>>());
        let vals = Int32Array::from((1..1000000).into_iter().collect::<Vec<i32>>());
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(ids) as ArrayRef),
            ("val", Arc::new(vals) as ArrayRef),
        ])
        .unwrap();

        writer.write(&batch).expect("Writing batch");

        // writer must be closed to write footer
    }
    writer.close().unwrap();
}
