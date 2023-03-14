use std::fs::File;

use arrow2::{
    array::{Array, Int32Array},
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use arrow2::array::{ListArray, PrimitiveArray};
use arrow2::buffer::Buffer;
use arrow2::datatypes::DataType;

fn write_chunk(path: &str, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}

fn main() -> Result<()> {
    let values = Buffer::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let values = PrimitiveArray::<i32>::new(DataType::Int32, values, None);

    let data_type = ListArray::<i32>::default_datatype(DataType::Int32);
    let array = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 4, 7, 7, 8, 10].try_into().unwrap(),
        Box::new(values),
        None,
    );

    let data_type = ListArray::<i32>::default_datatype(array.data_type().clone());
    let nested = ListArray::<i32>::new(
        data_type,
        vec![0, 2, 5, 6].try_into().unwrap(),
        Box::new(array),
        None,
    );
    let array2 = Int32Array::from(&[
        Some(0),
        Some(1),
        Some(2),
    ]);
    let field = Field::new("c1", nested.data_type().clone(), true);
    let field2 = Field::new("c2", array2.data_type().clone(), true);
    let schema = Schema::from(vec![field/*, field2*/]);
    let chunk = Chunk::new(vec![nested.boxed()/*,array2.boxed()*/]);

    write_chunk("test.parquet", schema, chunk)
}
