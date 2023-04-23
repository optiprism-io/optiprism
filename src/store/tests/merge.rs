#![feature(trace_macros)]

use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use rstest::rstest;

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
use store::test_util::{create_list_primitive_array, create_parquet_from_chunks, PrimaryIndexType};
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

struct TestCase {
    idx_fields: Vec<Field>,
    primary_index_type: PrimaryIndexType,
    data_fields: Vec<Field>,
    gen_null_values_periodicity: Option<usize>,
    gen_missing_cols_periodicity: Option<usize>,
    gen_exclusive_row_groups_periodicity: Option<usize>,
    gen_out_streams_count: usize,
    gen_row_group_size: usize,
    gen_data_page_limit: Option<usize>,
    out_data_page_size_limit: usize,
    out_row_group_values_limit: usize,
    out_arrow_page_size: usize,
}

fn test_merge(tc: TestCase) -> anyhow::Result<()> {
    let idx_cols_len = tc.idx_fields.len();
    let fields = [tc.idx_fields, tc.data_fields].concat();

    let names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let initial_chunk = gen_chunk_for_parquet(&fields, idx_cols_len, tc.primary_index_type, tc.gen_null_values_periodicity);
    trace!("original chunk");
    trace!("{}", print::write(&[initial_chunk.clone()], &names));
    let out_chunks = unmerge_chunk(
        initial_chunk.clone(),
        tc.gen_out_streams_count,
        tc.gen_row_group_size,
        tc.gen_exclusive_row_groups_periodicity,
    );

    for (stream_id, chunks) in out_chunks.iter().enumerate() {
        for (chunk_id,chunk) in chunks.iter().enumerate(){
            trace!("stream #{stream_id} out chunk #{chunk_id}");
            trace!("{}", print::write(&[chunk.clone()], &names));
        }
    }

    let readers = out_chunks
        .into_iter()
        .map(|chunks| {
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunks(
                chunks,
                fields.clone(),
                &mut w,
                tc.gen_data_page_limit,
            )
                .unwrap();

            w
        })
        .collect::<Vec<_>>();

    let mut out = Cursor::new(vec![]);
    let mut merger = Merger::try_new(
        readers,
        &mut out,
        idx_cols_len,
        tc.out_data_page_size_limit,
        tc.out_row_group_values_limit,
        tc.out_arrow_page_size,
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

    trace!(
        "final merged \n{}",
        print::write(&[final_chunk.clone()], &names)
    );

    debug_assert_eq!(initial_chunk, final_chunk);

    Ok(())
}

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
        Field::new("f13", DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new("f14", DataType::Timestamp(TimeUnit::Millisecond, Some("Utc".to_string())), true),
        Field::new("f15", DataType::Date32, true),
        Field::new("f16", DataType::Date64, true),
        Field::new("f17", DataType::Time32(TimeUnit::Millisecond), true),
        Field::new("f18", DataType::Time64(TimeUnit::Microsecond), true),
        Field::new("f19", DataType::Duration(TimeUnit::Millisecond), true),
        Field::new("f20", DataType::Binary, true),
        Field::new("f21", DataType::LargeBinary, true),
        Field::new("f22", DataType::Utf8, true),
        Field::new("f23", DataType::LargeUtf8, true),
        // Field::new("f24", DataType::Interval(IntervalUnit::DayTime), true), // TODO: support interval
        // Field::new("f25",DataType::FixedSizeBinary(10), true), // TODO: support fixed size binary
    ];

    let data_list_fields = data_fields.iter().map(|field| {
        let inner = Field::new("item", field.data_type.clone(), field.is_nullable);
        Field::new(format!("list_{}", field.name), DataType::List(Box::new(inner)), true)
    }).collect::<Vec<_>>();
    let data_fields = [data_fields, data_list_fields].concat();

    let cases = vec![
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_missing_cols_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100),
            gen_out_streams_count: 2,
            gen_row_group_size: 5,
            gen_data_page_limit: Some(5),
            out_data_page_size_limit: 10,
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
            gen_exclusive_row_groups_periodicity: None,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: Some(1),
            gen_missing_cols_periodicity: Some(1),
            primary_index_type: PrimaryIndexType::Sequential(100),
            gen_out_streams_count: 2,
            gen_row_group_size: 5,
            gen_data_page_limit: Some(5),
            out_data_page_size_limit: 10,
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
            gen_exclusive_row_groups_periodicity: None,
        },
        TestCase {
            idx_fields: vec![
                Field::new("idx1", DataType::Int64, false),
                Field::new("idx2", DataType::Int64, false),
            ],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: Some(1),
            gen_missing_cols_periodicity: Some(2),
            primary_index_type: PrimaryIndexType::Partitioned(10),
            gen_out_streams_count: 2,
            gen_row_group_size: 5,
            gen_data_page_limit: Some(5),
            out_data_page_size_limit: 10,
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
            gen_exclusive_row_groups_periodicity: None,
        },
    ];

    for case in cases.into_iter() {
        test_merge(case)?;
    }

    Ok(())
}