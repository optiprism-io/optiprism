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
use store::test_util::parse_markdown_table;

fn create_parquet_file(
    path: impl AsRef<Path>,
    row_groups: Vec<Vec<Vec<Box<dyn Array>>>>,
    fnames: Vec<&str>,
) -> anyhow::Result<()> {
    let mut file = std::fs::File::create(path)?;

    let fields = row_groups[0]
        .iter()
        .zip(fnames.iter())
        .map(|(arr, name)| Field::new(*name, arr[0].data_type().to_owned(), true))
        .collect::<Vec<_>>();

    let parquet_schema = to_parquet_schema(&Schema::from(fields))?;
    let mut seq_writer = FileSeqWriter::new(
        file,
        parquet_schema,
        WriteOptions {
            write_statistics: true,
            version: Version::V2,
        },
        None,
    );

    for row_group in row_groups.into_iter() {
        for col in row_group.into_iter() {
            for (page, fname) in col.into_iter().zip(fnames.iter()) {
                let f = Field::new(*fname, page.data_type().to_owned(), true);
                if let ParquetType::PrimitiveType(pt) = to_parquet_type(&f)? {
                    let mut pages = arrays_to_pages(&[page], vec![pt], vec![])?;
                    seq_writer.write_page(&CompressedPage::Data(pages.pop().unwrap()))?;
                } else {
                    panic!("not a primitive type");
                }
            }
            seq_writer.end_column()?;
        }
        seq_writer.end_row_group()?;
    }
    seq_writer.end(None)?;
    Ok(())
}


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
    // create_parquet_file("/tmp/optiprism/test.parquet", data, vec!["a", "b", "c"])?;

    Ok(())
}
