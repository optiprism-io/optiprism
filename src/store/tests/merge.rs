#![feature(trace_macros)]

use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

use arrow2::array::Array;
use arrow2::array::BinaryArray;
use arrow2::array::BooleanArray;
use arrow2::array::FixedSizeBinaryArray;
use arrow2::array::Float64Array;
use arrow2::array::Int128Array;
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
use arrow2::datatypes::IntervalUnit;
use arrow2::datatypes::Schema;
use arrow2::datatypes::TimeUnit;
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
use rstest::rstest;
use store::error::Result;
use store::merge::merger::Merger;
use store::merge::parquet::CompressedPageIterator;
use store::test_util::concat_chunks;
use store::test_util::create_list_primitive_array;
use store::test_util::create_parquet_file_from_chunk;
use store::test_util::create_parquet_from_chunk;
use store::test_util::create_parquet_from_chunks;
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
use store::test_util::make_missing_columns;
use store::test_util::make_missing_fields;
use store::test_util::parse_markdown_table;
use store::test_util::read_parquet;
use store::test_util::read_parquet_as_one_chunk;
use store::test_util::unmerge_chunk;
use store::test_util::PrimaryIndexType;
use tracing::trace;
use tracing::warn;
use tracing_test::traced_test;

struct TestCase {
    idx_fields: Vec<Field>,
    primary_index_type: PrimaryIndexType,
    data_fields: Vec<Field>,
    gen_null_values_periodicity: Option<usize>,
    gen_exclusive_row_groups_periodicity: Option<usize>,
    gen_out_streams_count: usize,
    gen_row_group_size: usize,
    gen_data_page_limit: Option<usize>,
    out_data_page_size_limit: Option<usize>,
    out_row_group_values_limit: usize,
    out_arrow_page_size: usize,
}

fn roundtrip(tc: TestCase) -> anyhow::Result<()> {
    let idx_cols_len = tc.idx_fields.len();
    let fields = [tc.idx_fields, tc.data_fields].concat();

    let names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let initial_chunk = gen_chunk_for_parquet(
        &fields,
        idx_cols_len,
        tc.primary_index_type,
        tc.gen_null_values_periodicity,
    );
    trace!("original chunk");
    trace!("{}", print::write(&[initial_chunk.clone()], &names));
    let out_streams_chunks = unmerge_chunk(
        initial_chunk.clone(),
        tc.gen_out_streams_count,
        tc.gen_row_group_size,
        tc.gen_exclusive_row_groups_periodicity,
    );

    for (stream_id, chunks) in out_streams_chunks.iter().enumerate() {
        for (chunk_id, chunk) in chunks.iter().enumerate() {
            trace!("stream #{stream_id} out chunk #{chunk_id}");
            trace!("{}", print::write(&[chunk.clone()], &names));
        }
    }

    let readers = out_streams_chunks
        .into_iter()
        .enumerate()
        .map(|(stream_id, chunks)| {
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunks(chunks, fields.clone(), &mut w, tc.gen_data_page_limit)
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
    let start = Instant::now();
    merger.merge()?;
    println!("{:?}", start.elapsed());

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
    let final_chunk = concat_chunks(chunks);

    trace!(
        "final merged \n{}",
        print::write(&[final_chunk.clone()], &names)
    );

    println!("{}", initial_chunk.len());
    // notice: if chunks look equal, but assert fails, probably data types are different
    debug_assert_eq!(initial_chunk, final_chunk);

    Ok(())
}

// #[traced_test]
#[test]
fn test_merge() -> anyhow::Result<()> {
    let data_fields = vec![
        Field::new("f1", DataType::Boolean, true),
        Field::new("f2", DataType::Int8, true),
        Field::new("f3", DataType::Int16, true),
        Field::new("f4", DataType::Int32, false),
        Field::new("f5", DataType::Int64, true),
        Field::new("f6", DataType::UInt8, true),
        Field::new("f7", DataType::UInt16, false),
        Field::new("f9", DataType::UInt32, true),
        Field::new("f10", DataType::UInt64, true),
        Field::new("f11", DataType::Float32, true),
        Field::new("f12", DataType::Float64, true),
        Field::new(
            "f13",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "f14",
            DataType::Timestamp(TimeUnit::Millisecond, Some("Utc".to_string())),
            true,
        ),
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

    let data_list_fields = data_fields
        .iter()
        .map(|field| {
            let inner = Field::new("item", field.data_type.clone(), field.is_nullable);
            Field::new(
                format!("list_{}", field.name),
                DataType::List(Box::new(inner)),
                true,
            )
        })
        .collect::<Vec<_>>();
    let data_fields = [data_fields, data_list_fields].concat();

    let cases = vec![
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100),
            gen_out_streams_count: 10,
            gen_row_group_size: 5,
            gen_data_page_limit: None,
            out_data_page_size_limit: Some(10),
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100),
            gen_out_streams_count: 2,
            gen_row_group_size: 5,
            gen_data_page_limit: Some(5),
            out_data_page_size_limit: Some(10),
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
        },
        TestCase {
            idx_fields: vec![
                Field::new("idx1", DataType::Int64, false),
                Field::new("idx2", DataType::Int64, false),
            ],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: Some(5),
            gen_exclusive_row_groups_periodicity: Some(3),
            primary_index_type: PrimaryIndexType::Partitioned(10),
            gen_out_streams_count: 2,
            gen_row_group_size: 5,
            gen_data_page_limit: Some(5),
            out_data_page_size_limit: Some(10),
            out_row_group_values_limit: 2,
            out_arrow_page_size: 3,
        },
        TestCase {
            idx_fields: vec![
                Field::new("idx1", DataType::Int64, false),
                Field::new("idx2", DataType::Int64, false),
            ],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Partitioned(1000),
            gen_out_streams_count: 10,
            gen_row_group_size: 1000,
            gen_data_page_limit: None,
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Partitioned(1000),
            gen_out_streams_count: 10,
            gen_row_group_size: 1000,
            gen_data_page_limit: None,
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100_000),
            gen_out_streams_count: 10,
            gen_row_group_size: 1000,
            gen_data_page_limit: None,
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(1_000_000),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: None,
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(1_000_000),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(100),
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: Some(2),
            primary_index_type: PrimaryIndexType::Sequential(1_000_000),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(100),
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
    ];

    for case in cases.into_iter() {
        roundtrip(case)?;
    }

    Ok(())
}

#[traced_test]
#[test]
fn test_different_row_group_sizes() -> anyhow::Result<()> {
    let streams = 3;
    let len = 100;
    let cols = 3;

    let fields = (0..cols)
        .into_iter()
        .map(|idx| Field::new(format!("f{}", idx).as_str(), DataType::Int64, idx > 0))
        .collect::<Vec<_>>();

    let names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let chunks = (0..streams)
        .into_iter()
        .map(|stream_id| {
            let arrs = (0..cols)
                .into_iter()
                .map(|_| {
                    let vec = (0..len)
                        .into_iter()
                        .step_by(streams)
                        .filter_map(|idx| {
                            if idx + (stream_id as i64) < len {
                                Some(idx + stream_id as i64)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    PrimitiveArray::<i64>::from_slice(vec.as_slice()).boxed()
                })
                .collect::<Vec<_>>();

            Chunk::new(arrs)
        })
        .collect::<Vec<_>>();

    let readers = chunks
        .into_iter()
        .enumerate()
        .map(|(stream_id, chunk)| {
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunk(
                chunk,
                fields.clone(),
                &mut w,
                Some(2),
                (stream_id + 1) * (stream_id + 1),
            )
            .unwrap();

            w
        })
        .collect::<Vec<_>>();

    let mut out = Cursor::new(vec![]);
    let mut merger = Merger::try_new(readers, &mut out, 1, None, 100, 100)?;

    merger.merge()?;

    let final_chunk = read_parquet_as_one_chunk(&mut out);

    trace!(
        "final merged \n{}",
        print::write(&[final_chunk.clone()], &names)
    );

    let exp = (0..cols)
        .into_iter()
        .map(|_| {
            let vec = (0..len).into_iter().collect::<Vec<_>>();

            PrimitiveArray::<i64>::from_slice(vec.as_slice()).boxed()
        })
        .collect::<Vec<_>>();

    let exp = Chunk::new(exp);
    debug_assert_eq!(exp, final_chunk);

    Ok(())
}

#[traced_test]
#[test]
fn test_missing_columns() -> anyhow::Result<()> {
    let cols = vec![
        vec![
            PrimitiveArray::<i64>::from_slice(&[1, 4, 7]).boxed(),
            PrimitiveArray::<i64>::from_slice(&[1, 4, 7]).boxed(),
            PrimitiveArray::<i64>::from_slice(&[1, 4, 7]).boxed(),
        ],
        vec![
            PrimitiveArray::<i64>::from_slice(&[2, 5, 8]).boxed(),
            PrimitiveArray::<i64>::from_slice(&[2, 5, 8]).boxed(),
        ],
        vec![
            PrimitiveArray::<i64>::from_slice(&[3, 6, 9]).boxed(),
            PrimitiveArray::<i64>::from_slice(&[3, 6, 9]).boxed(),
        ],
    ];

    let fields = vec![
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f2", DataType::Int64, true),
            Field::new("f3", DataType::Int64, true),
        ],
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f2", DataType::Int64, true),
        ],
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f3", DataType::Int64, true),
        ],
    ];

    let names = vec!["f1", "f2", "f3"];
    let readers = cols
        .into_iter()
        .zip(fields.iter())
        .map(|(cols, fields)| {
            let chunk = Chunk::new(cols);
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunk(chunk, fields.to_owned(), &mut w, None, 100).unwrap();

            w
        })
        .collect::<Vec<_>>();

    let mut out = Cursor::new(vec![]);
    let mut merger = Merger::try_new(readers, &mut out, 1, None, 100, 100)?;
    merger.merge()?;
    let final_chunk = read_parquet_as_one_chunk(&mut out);

    trace!(
        "final merged\n{}",
        print::write(&[final_chunk.clone()], &names)
    );

    let exp = vec![
        PrimitiveArray::<i64>::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9]).boxed(),
        PrimitiveArray::<i64>::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
            None,
            Some(7),
            Some(8),
            None,
        ])
        .boxed(),
        PrimitiveArray::<i64>::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            None,
            Some(9),
        ])
        .boxed(),
    ];

    let exp = Chunk::new(exp);

    trace!("expected\n{}", print::write(&[exp.clone()], &names));
    debug_assert_eq!(exp, final_chunk);

    Ok(())
}
