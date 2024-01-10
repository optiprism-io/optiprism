use std::fs::File;

use arrow2::array::Int32Array;
use arrow2::chunk::Chunk;
use arrow2::compute::arithmetics;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::write::CompressionOptions;
use arrow2::io::parquet::write::Encoding;
use arrow2::io::parquet::write::FileWriter;
use arrow2::io::parquet::write::RowGroupIterator;
use arrow2::io::parquet::write::Version;
use arrow2::io::parquet::write::WriteOptions;

fn main() {
    // declare arrays
    let a = Int32Array::from(&[Some(1), None, Some(3)]);
    let b = Int32Array::from(&[Some(2), None, Some(6)]);

    // compute (probably the fastest implementation of a nullable op you can find out there)
    let c = arithmetics::basic::mul_scalar(&a, &2);
    assert_eq!(c, b);

    // declare a schema with fields
    let schema = Schema::from(vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
    ]);

    // declare chunk
    let chunk = Chunk::new(vec![a.arced(), b.arced()]);

    // write to parquet (probably the fastest implementation of writing to parquet out there)

    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Snappy,
        version: Version::V1,
        data_pagesize_limit: None,
    };

    let row_groups =
        RowGroupIterator::try_new(vec![Ok(chunk)].into_iter(), &schema, options, vec![
            vec![Encoding::Plain],
            vec![Encoding::Plain],
        ])
        .unwrap();

    // anything implementing `std::io::Write` works
    let mut file = File::create("/tmp/test.parquet").unwrap();

    let mut writer = FileWriter::try_new(file, schema, options).unwrap();

    // Write the file.
    for group in row_groups {
        writer.write(group.unwrap()).unwrap();
    }
    let _ = writer.end(None).unwrap();
}
