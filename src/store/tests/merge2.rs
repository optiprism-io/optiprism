use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;

use arrow2::array::Array;
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::FixedSizeBinaryArray;
use arrow2::array::Float64Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::Int8Array;
use arrow2::array::ListArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutableListArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::PrimitiveArray;
use arrow2::array::TryExtend;
use arrow2::array::Utf8Array;
use arrow2::buffer::Buffer;
use arrow2::chunk::Chunk;
use arrow2::compute::concatenate::concatenate;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io::csv::read::deserialize_batch;
use arrow2::io::csv::read::deserialize_column;
use arrow2::io::csv::read::infer;
use arrow2::io::csv::read::infer_schema;
use arrow2::io::csv::read::read_rows;
use arrow2::io::csv::read::ByteRecord;
use arrow2::io::csv::read::ReaderBuilder;
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::to_parquet_schema;
use arrow2::io::parquet::write::to_parquet_type;
use arrow2::io::parquet::write::transverse;
use arrow2::io::parquet::write::FileWriter;
use arrow2::io::parquet::write::RowGroupIterator;
use arrow2::io::print;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use arrow_schema::DataType::LargeUtf8;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::page::CompressedPage;
use parquet2::read::get_page_iterator;
use parquet2::read::read_metadata;
use parquet2::schema::types::ParquetType;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;
use store::error::Result;
use store::parquet_new::merger::Merger;
use store::parquet_new::parquet::CompressedPageIterator;
use store::test_util::create_list_primitive_array;
use store::test_util::create_parquet_file_from_chunk;
use store::test_util::create_parquet_from_chunk;
use store::test_util::gen_binary_data_array;
use store::test_util::gen_binary_data_list_array;
use store::test_util::gen_boolean_data_array;
use store::test_util::gen_boolean_data_list_array;
use store::test_util::gen_chunk_for_parquet;
use store::test_util::gen_fixed_size_binary_data_array;
use store::test_util::gen_idx_primitive_array;
use store::test_util::gen_primitive_data_array;
use store::test_util::gen_primitive_data_list_array;
use store::test_util::gen_secondary_idx_primitive_array;
use store::test_util::gen_utf8_data_array;
use store::test_util::gen_utf8_data_list_array;
use store::test_util::parse_markdown_table;
use store::test_util::unmerge_chunk;
use tracing::trace;
use tracing::warn;
use tracing_test::traced_test;

#[traced_test]
#[test]
fn test() -> anyhow::Result<()> {
    let fields = vec![
        Field::new("idx1", DataType::Int64, false),
        Field::new("idx2", DataType::Int32, false),
        Field::new("d1", DataType::UInt64, true),
        Field::new("d2", DataType::Float64, true),
        Field::new("d3", DataType::Utf8, true),
        Field::new("d4", DataType::LargeUtf8, true),
        Field::new("d5", DataType::Binary, true),
        Field::new("d6", DataType::LargeBinary, true),
        Field::new("d7", DataType::Boolean, true),
        Field::new(
            "dl1",
            DataType::List(Box::new(Field::new("f", DataType::Int64, false))),
            true,
        ),
        Field::new(
            "dl2",
            DataType::List(Box::new(Field::new("f", DataType::Float64, false))),
            true,
        ),
        Field::new(
            "dl3",
            DataType::List(Box::new(Field::new("f", DataType::Utf8, false))),
            true,
        ),
        Field::new(
            "dl4",
            DataType::List(Box::new(Field::new("f", DataType::LargeUtf8, false))),
            true,
        ),
        Field::new(
            "dl5",
            DataType::List(Box::new(Field::new("f", DataType::Binary, false))),
            true,
        ),
        Field::new(
            "dl6",
            DataType::List(Box::new(Field::new("f", DataType::LargeBinary, false))),
            true,
        ),
        Field::new(
            "dl7",
            DataType::List(Box::new(Field::new("f", DataType::Boolean, false))),
            true,
        ),
    ];
    let names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let initial_chunk = gen_chunk_for_parquet(&fields, 2, 10, None);
    let chunk_len = initial_chunk.len();
    println!("original chunk");
    println!("{}", print::write(&[initial_chunk.clone()], &names));
    let out_count = 3;
    // let
    // let row_group_size = chunk_len / out_count / 5;
    let row_group_size = 5;
    let data_page_limit = Some(5);
    let out_chunks = unmerge_chunk(initial_chunk.clone(), out_count, row_group_size);
    for (idx, c) in out_chunks.iter().enumerate() {
        println!("out chunk #{idx}");
        println!("{}", print::write(&[c.clone()], &names));
    }
    assert_eq!(chunk_len, out_chunks.iter().map(|c| c.len()).sum::<usize>());

    let readers = out_chunks
        .into_iter()
        .map(|chunk| {
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunk(
                chunk,
                fields.clone(),
                &mut w,
                data_page_limit,
                row_group_size,
            )
                .unwrap();
            let metadata = read::read_metadata(&mut w).unwrap();
            for f in metadata.schema().fields().iter() {
                println!("- {:?}", f);
            }
            let schema = read::infer_schema(&metadata).unwrap();
            for f in schema.fields.iter() {
                println!("{} {:?}", f.name, f.data_type);
            }
            w
        })
        .collect::<Vec<_>>();

    let mut out = Cursor::new(vec![]);
    let data_page_size_limit = 10;
    let row_group_values_limit = 2;
    let arrow_page_size = 3;
    let mut merger = Merger::try_new(
        readers,
        &mut out,
        2,
        data_page_size_limit,
        row_group_values_limit,
        arrow_page_size,
        "merged".to_string(),
    )?;
    merger.merge()?;

    let metadata = read::read_metadata(&mut out)?;
    let schema = read::infer_schema(&metadata)?;
    for f in schema.fields.iter() {
        println!("final {} {:?}", f.name, f.data_type);
    }
    // println!("{schema:?}");
    // we can then read the row groups into chunks
    let mut chunks = read::FileReader::new(
        out,
        metadata.row_groups,
        schema,
        Some(1024 * 1024),
        None,
        None,
    );

    let chunks = chunks.map(|chunk| chunk.unwrap()).collect::<Vec<_>>();
    let arrs = (0..fields.len())
        .into_iter()
        .map(|arr_id| {
            let to_concat = chunks
                .iter()
                .map(|chunk| chunk.arrays()[arr_id].as_ref())
                .collect::<Vec<_>>();
            concatenate(&to_concat).unwrap()
        })
        .collect::<Vec<_>>();

    let final_chunk = Chunk::new(arrs);

    println!(
        "final merged \n{}",
        print::write(&[final_chunk.clone()], &names)
    );
    assert_eq!(initial_chunk, final_chunk);
    Ok(())
}

fn write_chunk(w: impl Write, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = arrow2::io::parquet::write::WriteOptions {
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

    let mut writer = FileWriter::try_new(w, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}

#[traced_test]
#[test]
fn test2() {
    let arrs = vec![
        Int64Array::from(&[Some(0), Some(1)]).boxed(),
        Int64Array::from(&[Some(0), Some(1)]).boxed(),
        Int64Array::from(&[Some(1232222123), Some(3453222245)]).boxed(),
    ];

    let schema = Schema::from(vec![
        Field::new("idx1", arrs[0].data_type().clone(), true),
        Field::new("idx2", arrs[1].data_type().clone(), true),
        Field::new("f", DataType::Date64, true),
    ]);

    let chunk = Chunk::new(arrs);
    let mut out1 = Cursor::new(vec![]);
    write_chunk(&mut out1, schema.clone(), chunk.clone()).unwrap();

    let metadata = read::read_metadata(&mut out1).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();
    for f in schema.fields.iter() {
        println!("out1 {} {:?}", f.name, f.data_type);
    }

    let mut out2 = Cursor::new(vec![]);
    write_chunk(&mut out2, schema, chunk).unwrap();

    let mut merged = Cursor::new(vec![]);
    let mut merger = Merger::try_new(
        vec![out1, out2],
        &mut merged,
        2,
        2,
        2,
        2,
        "merged".to_string(),
    )
        .unwrap();
    merger.merge().unwrap();

    let metadata = read::read_metadata(&mut merged).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();
    for f in schema.fields.iter() {
        println!("final {} {:?}", f.name, f.data_type);
    }

    let mut chunks = read::FileReader::new(
        merged,
        metadata.row_groups,
        schema,
        Some(1024 * 1024),
        None,
        None,
    );

    for chunk in chunks {
        println!("{:?}", chunk.unwrap());
    }
}
