#![feature(trace_macros)]

use std::fs;
use std::fs::create_dir_all;
use std::fs::File;
use std::io::Cursor;
use std::path::PathBuf;
use std::time::Instant;

use arrow2::array::PrimitiveArray;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::TimeUnit;
use arrow2::io::parquet::read;
use arrow2::io::print;
use storage::arrow_conversion::arrow1_to_arrow2;
use storage::parquet::parquet_merger::merge;
use storage::parquet::parquet_merger::Options;
use storage::test_util::concat_chunks;
use storage::test_util::create_parquet_from_chunk;
use storage::test_util::create_parquet_from_chunks;
use storage::test_util::gen_chunk_for_parquet;
use storage::test_util::parse_markdown_tables;
use storage::test_util::read_parquet_as_one_chunk;
use storage::test_util::unmerge_chunk;
use storage::test_util::PrimaryIndexType;
use tracing::trace;
use tracing_test::traced_test;

#[derive(Clone)]
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

    let _names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let initial_chunk = gen_chunk_for_parquet(
        &fields,
        idx_cols_len,
        tc.primary_index_type,
        tc.gen_null_values_periodicity,
    );
    // trace!("original chunk");
    // trace!("{}", print::write(&[initial_chunk.clone()], &names));
    let out_streams_chunks = unmerge_chunk(
        initial_chunk.clone(),
        tc.gen_out_streams_count,
        tc.gen_row_group_size,
        tc.gen_exclusive_row_groups_periodicity,
    );

    let readers = out_streams_chunks
        .into_iter()
        .enumerate()
        .map(|(_stream_id, chunks)| {
            let mut w = Cursor::new(vec![]);
            create_parquet_from_chunks(chunks, fields.clone(), &mut w, tc.gen_data_page_limit)
                .unwrap();

            w
        })
        .collect::<Vec<_>>();

    let opts = Options {
        index_cols: idx_cols_len,
        is_replacing: false,
        data_page_size_limit_bytes: tc.out_data_page_size_limit,
        row_group_values_limit: tc.out_row_group_values_limit,
        array_page_size: tc.out_arrow_page_size,
        out_part_id: 0,
        merge_max_page_size: 1024 * 1024,
        max_part_size_bytes: None,
    };

    let start = Instant::now();
    fs::remove_dir_all("/tmp/merge_roundtrip").ok();
    fs::create_dir_all("/tmp/merge_roundtrip").unwrap();
    merge(readers, "/tmp/merge_roundtrip".into(), 1, "t", 0, opts).unwrap();
    println!("merge {:?}", start.elapsed());

    let mut pfile = File::open("/tmp/merge_roundtrip/1.parquet").unwrap();
    let metadata = read::read_metadata(&mut pfile).unwrap();
    let schema = read::infer_schema(&metadata).unwrap();
    let chunks = read::FileReader::new(
        File::open("/tmp/merge_roundtrip/1.parquet").unwrap(),
        metadata.row_groups,
        schema,
        Some(1024 * 1024),
        None,
        None,
    );

    let chunks = chunks.map(|chunk| chunk.unwrap()).collect::<Vec<_>>();
    let final_chunk = concat_chunks(chunks);

    // notice: if chunks look equal, but assert fails, probably data types are different
    debug_assert_eq!(initial_chunk, final_chunk);

    Ok(())
}
#[allow(dead_code)]
enum ProfileStep {
    Generate,
    Merge,
}

#[allow(dead_code)]
fn stream_parquet_path(stream_id: usize, case_id: usize) -> PathBuf {
    // let mut path = temp_dir();
    // path.push(format!("optiprism/tests/merge"));
    let mut path = PathBuf::from(format!("/tmp/merge/{case_id}/"));
    // path.push(format!("/tmp/merge/{case_id}/"));
    create_dir_all(path.clone()).unwrap();
    path.push(format!("{stream_id}.parquet"));

    path
}
#[allow(dead_code)]
fn profile(tc: TestCase, case_id: usize, step: ProfileStep) {
    let start = Instant::now();
    match step {
        ProfileStep::Generate => {
            let idx_cols_len = tc.idx_fields.len();
            let fields = [tc.idx_fields, tc.data_fields].concat();

            let initial_chunk = gen_chunk_for_parquet(
                &fields,
                idx_cols_len,
                tc.primary_index_type,
                tc.gen_null_values_periodicity,
            );
            let start = Instant::now();
            let out_streams_chunks = unmerge_chunk(
                initial_chunk.clone(),
                tc.gen_out_streams_count,
                tc.gen_row_group_size,
                tc.gen_exclusive_row_groups_periodicity,
            );

            println!("unmerge {:?}", start.elapsed());

            let start = Instant::now();
            out_streams_chunks
                .into_iter()
                .enumerate()
                .for_each(|(_stream_id, chunks)| {
                    let mut w = Cursor::new(vec![]);
                    create_parquet_from_chunks(
                        chunks,
                        fields.clone(),
                        &mut w,
                        tc.gen_data_page_limit,
                    )
                    .unwrap();
                });
            println!("create parquets {:?}", start.elapsed());
        }
        ProfileStep::Merge => {
            let idx_cols_len = tc.idx_fields.len();
            let readers = (0..tc.gen_out_streams_count)
                .map(|stream_id| File::open(stream_parquet_path(stream_id, case_id)).unwrap())
                .collect::<Vec<_>>();

            let opts = Options {
                index_cols: idx_cols_len,
                is_replacing: false,
                data_page_size_limit_bytes: tc.out_data_page_size_limit,
                row_group_values_limit: tc.out_row_group_values_limit,
                array_page_size: tc.out_arrow_page_size,
                out_part_id: 0,
                merge_max_page_size: 100,
                max_part_size_bytes: None,
            };

            fs::remove_dir_all("/tmp/merge_profile").ok();
            fs::create_dir_all("/tmp/merge_profile").unwrap();
            merge(readers, "/tmp/merge_profile".into(), 1, "t", 0, opts).unwrap();
        }
    }
    println!("{:?}", start.elapsed());
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
        Field::new("f17", DataType::Binary, true),
        Field::new("f18", DataType::LargeBinary, true),
        Field::new("f19", DataType::Utf8, true),
        Field::new("f20", DataType::LargeUtf8, true),
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
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Sequential(100),
        // gen_out_streams_count: 2,
        // gen_row_group_size: 5,
        // gen_data_page_limit: Some(5),
        // out_data_page_size_limit: Some(10),
        // out_row_group_values_limit: 2,
        // out_arrow_page_size: 3,
        // },
        // TestCase {
        // idx_fields: vec![
        // Field::new("idx1", DataType::Int64, false),
        // Field::new("idx2", DataType::Int64, false),
        // ],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: Some(5),
        // gen_exclusive_row_groups_periodicity: Some(3),
        // primary_index_type: PrimaryIndexType::Partitioned(10),
        // gen_out_streams_count: 2,
        // gen_row_group_size: 5,
        // gen_data_page_limit: Some(5),
        // out_data_page_size_limit: Some(10),
        // out_row_group_values_limit: 2,
        // out_arrow_page_size: 3,
        // },
        // TestCase {
        // idx_fields: vec![
        // Field::new("idx1", DataType::Int64, false),
        // Field::new("idx2", DataType::Int64, false),
        // ],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Partitioned(1000),
        // gen_out_streams_count: 10,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: None,
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Partitioned(1000),
        // gen_out_streams_count: 10,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: None,
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Sequential(100_000),
        // gen_out_streams_count: 10,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: None,
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Sequential(1_000_000),
        // gen_out_streams_count: 3,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: None,
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: None,
        // primary_index_type: PrimaryIndexType::Sequential(1_000_000),
        // gen_out_streams_count: 3,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: Some(100),
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        // TestCase {
        // idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
        // data_fields: data_fields.clone(),
        // gen_null_values_periodicity: None,
        // gen_exclusive_row_groups_periodicity: Some(2),
        // primary_index_type: PrimaryIndexType::Sequential(1_000_000),
        // gen_out_streams_count: 3,
        // gen_row_group_size: 1000,
        // gen_data_page_limit: Some(100),
        // out_data_page_size_limit: None,
        // out_row_group_values_limit: 1000,
        // out_arrow_page_size: 500,
        // },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(1000),
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![
                Field::new("idx1", DataType::Int64, false),
                Field::new("idx2", DataType::Int64, false),
            ],
            data_fields,
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: Some(2),
            primary_index_type: PrimaryIndexType::Partitioned(100),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(1000),
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

// #[test]
// todo Move to benches
#[allow(dead_code)]
fn test_profile_merge() {
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
        Field::new("f18", DataType::Time64(TimeUnit::Millisecond), true),
        Field::new("f19", DataType::Duration(TimeUnit::Millisecond), true),
        Field::new("f20", DataType::Binary, true),
        Field::new("f21", DataType::LargeBinary, true),
        Field::new("f22", DataType::Utf8, true),
        Field::new("f23", DataType::LargeUtf8, true),
        // Field::new("f24", DataType::Interval(IntervalUnit::DayTime), true), // TODO: support interval
        // Field::new("f25",DataType::FixedSizeBinary(10), true), // TODO: support fixed size binary
    ];

    //    let data_list_fields = data_fields
    // .iter()
    // .map(|field| {
    // let inner = Field::new("item", field.data_type.clone(), field.is_nullable);
    // Field::new(
    // format!("list_{}", field.name),
    // DataType::List(Box::new(inner)),
    // true,
    // )
    // })
    // .collect::<Vec<_>>();
    // let data_fields = [data_fields, data_list_fields].concat();

    let cases = vec![
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields: data_fields.clone(),
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: None,
            primary_index_type: PrimaryIndexType::Sequential(100_000),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(1000),
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
        TestCase {
            idx_fields: vec![Field::new("idx1", DataType::Int64, false)],
            data_fields,
            gen_null_values_periodicity: None,
            gen_exclusive_row_groups_periodicity: Some(2),
            primary_index_type: PrimaryIndexType::Sequential(100_000),
            gen_out_streams_count: 3,
            gen_row_group_size: 1000,
            gen_data_page_limit: Some(1000),
            out_data_page_size_limit: None,
            out_row_group_values_limit: 1000,
            out_arrow_page_size: 500,
        },
    ];

    for (idx, case) in cases.iter().enumerate() {
        profile(case.to_owned(), idx, ProfileStep::Generate);
    }

    for (idx, case) in cases.iter().enumerate() {
        profile(case.to_owned(), idx, ProfileStep::Merge);
    }
}

// #[traced_test]
#[test]
fn test_different_row_group_sizes() -> anyhow::Result<()> {
    let streams = 3;
    let len = 100;
    let cols = 3;

    let fields = (0..cols)
        .map(|idx| Field::new(format!("f{}", idx).as_str(), DataType::Int64, idx > 0))
        .collect::<Vec<_>>();

    let names = fields
        .iter()
        .map(|f| f.name.to_string())
        .collect::<Vec<_>>();

    let stream_chunks = (0..streams)
        .map(|stream_id| {
            let arrs = (0..cols)
                .map(|_| {
                    let vec = (0..len)
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

    let readers = stream_chunks
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

    let opts = Options {
        index_cols: 2,
        is_replacing: false,
        data_page_size_limit_bytes: None,
        row_group_values_limit: 100,
        array_page_size: 100,
        out_part_id: 0,
        merge_max_page_size: 100,
        max_part_size_bytes: None,
    };

    fs::remove_dir_all("/tmp/merge_different_row_group_sizes").ok();
    fs::create_dir_all("/tmp/merge_different_row_group_sizes").unwrap();
    merge(
        readers,
        "/tmp/merge_different_row_group_sizes".into(),
        1,
        "t",
        0,
        opts,
    )
    .unwrap();

    let mut pfile = File::open("/tmp/merge_different_row_group_sizes/1.parquet").unwrap();
    let final_chunk = read_parquet_as_one_chunk(&mut pfile);

    trace!(
        "final merged \n{}",
        print::write(&[final_chunk.clone()], &names)
    );

    let exp = (0..cols)
        .map(|_| {
            let vec = (0..len).collect::<Vec<_>>();

            PrimitiveArray::<i64>::from_slice(vec.as_slice()).boxed()
        })
        .collect::<Vec<_>>();

    let exp = Chunk::new(exp);
    debug_assert_eq!(exp, final_chunk);

    Ok(())
}

// #[traced_test]
#[test]
fn test_merge_with_missing_columns() -> anyhow::Result<()> {
    let cols = vec![
        vec![
            PrimitiveArray::<i64>::from_slice([1, 4, 7]).boxed(),
            PrimitiveArray::<i64>::from_slice([1, 4, 7]).boxed(),
            PrimitiveArray::<i64>::from_slice([1, 4, 7]).boxed(),
        ],
        vec![
            PrimitiveArray::<i64>::from_slice([2, 5, 8]).boxed(),
            PrimitiveArray::<i64>::from_slice([2, 5, 8]).boxed(),
        ],
        vec![
            PrimitiveArray::<i64>::from_slice([3, 6, 9]).boxed(),
            PrimitiveArray::<i64>::from_slice([3, 6, 9]).boxed(),
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

    let opts = Options {
        index_cols: 1,
        is_replacing: false,
        data_page_size_limit_bytes: None,
        row_group_values_limit: 100,
        array_page_size: 100,
        out_part_id: 0,
        merge_max_page_size: 100,
        max_part_size_bytes: None,
    };

    fs::remove_dir_all("/tmp/merge_with_missing_columns").ok();
    fs::create_dir_all("/tmp/merge_with_missing_columns").unwrap();
    merge(
        readers,
        "/tmp/merge_with_missing_columns".into(),
        1,
        "t",
        0,
        opts,
    )?;
    let mut pfile = File::open("/tmp/merge_with_missing_columns/1.parquet").unwrap();
    let final_chunk = read_parquet_as_one_chunk(&mut pfile);

    let exp = vec![
        PrimitiveArray::<i64>::from_slice([1, 2, 3, 4, 5, 6, 7, 8, 9]).boxed(),
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

#[traced_test]
#[test]
fn test_pick_with_null_columns() -> anyhow::Result<()> {
    let cols = vec![
        vec![
            PrimitiveArray::<i64>::from_slice([1, 2, 3]).boxed(),
            PrimitiveArray::<i64>::from_slice([1, 2, 3]).boxed(),
        ],
        vec![PrimitiveArray::<i64>::from_slice([4, 5, 6]).boxed()],
    ];

    let fields = vec![
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f2", DataType::Int64, true),
        ],
        vec![Field::new("f1", DataType::Int64, false)],
    ];

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

    fs::remove_dir_all("/tmp/merge_pick_with_null_columns").ok();
    fs::create_dir_all("/tmp/merge_pick_with_null_columns").unwrap();
    let opts = Options {
        index_cols: 1,
        is_replacing: false,
        data_page_size_limit_bytes: None,
        row_group_values_limit: 100,
        array_page_size: 100,
        out_part_id: 0,
        merge_max_page_size: 100,
        max_part_size_bytes: None,
    };
    merge(
        readers,
        "/tmp/merge_pick_with_null_columns".into(),
        1,
        "t",
        0,
        opts,
    )?;

    let mut pfile = File::open("/tmp/merge_pick_with_null_columns/1.parquet").unwrap();
    let final_chunk = read_parquet_as_one_chunk(&mut pfile);

    let exp = Chunk::new(vec![
        PrimitiveArray::<i64>::from_slice([1, 2, 3, 4, 5, 6]).boxed(),
        PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3), None, None, None]).boxed(),
    ]);

    debug_assert_eq!(exp, final_chunk);
    Ok(())
}

#[traced_test]
#[test]
fn test_replacing() -> anyhow::Result<()> {
    let cols = vec![
        vec![
            PrimitiveArray::<i64>::from_slice([1, 1, 1, 2, 2]).boxed(),
            PrimitiveArray::<i64>::from_slice([1, 2, 3, 1, 2]).boxed(),
        ],
        vec![
            PrimitiveArray::<i64>::from_slice([1, 1, 1, 2, 2]).boxed(),
            PrimitiveArray::<i64>::from_slice([1, 2, 3, 1, 2]).boxed(),
        ],
    ];

    let fields = vec![
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f2", DataType::Int64, true),
        ],
        vec![
            Field::new("f1", DataType::Int64, false),
            Field::new("f2", DataType::Int64, true),
        ],
    ];

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

    fs::remove_dir_all("/tmp/merge_replacing").ok();
    fs::create_dir_all("/tmp/merge_replacing").unwrap();
    let opts = Options {
        index_cols: 2,
        is_replacing: true,
        data_page_size_limit_bytes: None,
        row_group_values_limit: 100,
        array_page_size: 100,
        out_part_id: 0,
        merge_max_page_size: 100,
        max_part_size_bytes: None,
    };
    merge(readers, "/tmp/merge_replacing".into(), 1, "t", 0, opts)?;

    let mut pfile = File::open("/tmp/merge_replacing/1.parquet").unwrap();
    let final_chunk = read_parquet_as_one_chunk(&mut pfile);

    let exp = Chunk::new(vec![
        PrimitiveArray::<i64>::from_slice([1, 2]).boxed(),
        PrimitiveArray::<i64>::from_slice(vec![3, 2]).boxed(),
    ]);

    debug_assert_eq!(exp, final_chunk);
    Ok(())
}
