#![feature(trace_macros)]

use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;

use arrow2::array::{Array, Int128Array};
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
use arrow2::datatypes::{DataType, IntervalUnit, TimeUnit};
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
    let data_fields = vec![
        Field::new("f1", DataType::Boolean, true),
        Field::new("f2", DataType::Int8, true),
        Field::new("f3", DataType::Int16, true),
        Field::new("f4", DataType::Int32, true),
        Field::new("f5", DataType::Int64, true),
        Field::new("f6", DataType::UInt8, true),
        Field::new("f7", DataType::UInt16, true),
        Field::new("f9", DataType::UInt32, true),
        Field::new("f10", DataType::UInt64, true),
        Field::new("f11", DataType::Float32, true),
        Field::new("f12", DataType::Float64, true),
        Field::new("f13", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new("f14", DataType::Timestamp(TimeUnit::Second, Some("Utc".to_string())), true),
        Field::new("f15", DataType::Date32, true),
        Field::new("f16", DataType::Date64, true),
        Field::new("f17", DataType::Time32(TimeUnit::Second), true),
        Field::new("f18", DataType::Time64(TimeUnit::Second), true),
        Field::new("f19", DataType::Duration(TimeUnit::Second), true),
        Field::new("f20", DataType::Interval(IntervalUnit::YearMonth), true),
        Field::new("f21", DataType::Binary, true),
        Field::new("f22", DataType::FixedSizeBinary(32), true),
        Field::new("f23", DataType::LargeBinary, true),
        Field::new("f24", DataType::Utf8, true),
        Field::new("f25", DataType::LargeUtf8, true),
    ];

    let list_fields = data_fields.iter().map(|field| {
        Field::new(format!("list_{}", field.name()), DataType::List(Box::new(field.to_owned())), true)
    }).collect::<Vec<_>>();
    let large_list_fields = data_fields.iter().map(|field| {
        Field::new(format!("large_list_{}", field.name()), DataType::List(Box::new(field.to_owned())), true)
    }).collect::<Vec<_>>();

    let fields = [data_fields, list_fields, large_list_fields].concat();

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
    )?;
    merger.merge()?;

    let metadata = read::read_metadata(&mut out)?;
    let schema = read::infer_schema(&metadata)?;
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
    for idx in 0..initial_chunk.arrays().len() {
        println!("idx {idx}");
        println!("initial data type: {:?}", initial_chunk.arrays()[idx].data_type());
        println!("result data type: {:?}", final_chunk.arrays()[idx].data_type());
        debug_assert_eq!(initial_chunk.arrays()[idx], final_chunk.arrays()[idx]);
    }
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
        Int64Array::from_vec(vec![0, 1]).boxed(),
        Int128Array::from_vec(vec![1i128, 2]).to(DataType::Decimal(9, 1)).boxed(),
    ];

    println!("ppp {:?}", DataType::Decimal(39, 1).to_physical_type());
    let schema = Schema::from(vec![
        Field::new("idx1", arrs[0].data_type().clone(), true),
        Field::new("f", DataType::Decimal(9, 1), true),
    ]);

    let chunk = Chunk::new(arrs);
    let mut out1 = Cursor::new(vec![]);
    write_chunk(&mut out1, schema.clone(), chunk.clone()).unwrap();
    println!("?");
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
        1,
        2,
        4,
        2,
    )
        .unwrap();
    merger.merge().unwrap();

    let metadata = read::read_metadata(&mut merged).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();

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
