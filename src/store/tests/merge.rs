use std::io;
use std::io::Cursor;
use std::path::Path;
use std::io::{BufRead, BufReader};
use arrow2::array::{Array, BooleanArray, Float64Array, Int32Array, ListArray, MutableBooleanArray, MutableListArray, MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray};
use arrow2::array::{Int8Array, Int64Array};
use arrow2::array::Utf8Array;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use arrow2::datatypes::Schema;
use arrow2::io::csv::read::{ByteRecord, deserialize_batch, deserialize_column, infer, infer_schema, read_rows, ReaderBuilder};
use arrow2::io::parquet::write::{to_parquet_schema, to_parquet_type};
use arrow2::types::NativeType;
use parquet2::compression::CompressionOptions;
use parquet2::page::CompressedPage;
use parquet2::schema::types::ParquetType;
use parquet2::write::{FileSeqWriter, WriteOptions};
use parquet2::write::Version;
use store::error::Result;
use store::parquet::parquet::arrays_to_pages;
use arrow2::array::{TryExtend};
use arrow2::offset::Offset;
use store::test_util::{create_parquet_from_arrays, parse_markdown_table};

#[test]
fn test() -> anyhow::Result<()> {
    let data = r#"
+---+-------+------+-------+-------+
| a |     b |    c |     d | e     |
+---+-------+------+-------+-------+
| 1 |  true | test | 1,2,3 | a,b,c |
| 2 |       |      |   1,2 | b     |
| 3 | false | lala |       |       |
+---+-------+------+-------+-------+
    "#;
    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("c", DataType::Utf8, true),
        Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int32, true))), true),
        Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Utf8, true))), true),
    ];

    let parsed = parse_markdown_table(data, &fields)?;
    println!("{:#?}", parsed);
    create_parquet_from_arrays(parsed, "/tmp/optiprism/test.parquet", fields, 2, 2)?;
    // create_parquet_file("/tmp/optiprism/test.parquet", data, vec!["a", "b", "c"])?;

    Ok(())
}
