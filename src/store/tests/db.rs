#![feature(fs_try_exists)]

use std::fs;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::array::PrimitiveArray;
use arrow2::chunk::Chunk;
use common::types::DType;
use futures::Stream;
use futures::StreamExt;
use store::db::OptiDBImpl;
use store::db::Options;
use store::db::TableOptions;
use store::NamedValue;
use store::Value;

#[tokio::test]
async fn test_scenario() {
    let path = PathBuf::from("test_data/db_scenario");
    fs::remove_dir_all(&path).unwrap();
    fs::create_dir_all(&path).unwrap();

    let opts = Options {};
    let mut db = OptiDBImpl::open(path.clone(), opts).unwrap();
    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        partitions: 1,
        index_cols: 1,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 2,
        max_log_length_bytes: 1024 * 1024 * 100,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_index_cols: 1,
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024 * 8 * 8,
        merge_max_page_size: 1024 * 1024,
    };

    db.create_table("t1".to_string(), topts).unwrap();
    db.add_field("t1", "f1", DType::Int64, true).unwrap();

    db.insert("t1", vec![NamedValue::new(
        "f1".to_string(),
        Value::Int64(Some(1)),
    )])
    .unwrap();

    db.add_field("t1", "f2", DType::Int64, true).unwrap();

    db.insert("t1", vec![
        NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
        NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
    ])
    .unwrap();
    // scan two columns
    let stream = db
        .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(2)]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    // scan one column
    let stream = db.scan_partition("t1", 0, vec!["f1".to_string()]).unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2)]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    // new row
    db.insert("t1", vec![
        NamedValue::new("f1".to_string(), Value::Int64(Some(3))),
        NamedValue::new("f2".to_string(), Value::Int64(None)),
    ])
    .unwrap();

    // scan two columns
    let stream = db
        .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    let lpath = PathBuf::from("test_data/db_scenario/tables/t1/0000000000000000.log");
    assert!(fs::try_exists(lpath).unwrap());

    // stop current instance
    drop(db);

    // reopen db
    let opts = Options {};
    let mut db = OptiDBImpl::open(path.clone(), opts).unwrap();

    // scan two columns
    let stream = db
        .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    // flush§ to part
    db.flush().unwrap();
    let ppath = PathBuf::from("test_data/db_scenario/tables/t1/0/0/0.parquet");
    assert!(fs::try_exists(&ppath).unwrap());

    // scan two columns
    let stream = db
        .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    // add one more row

    db.insert("t1", vec![
        NamedValue::new("f1".to_string(), Value::Int64(Some(4))),
        NamedValue::new("f2".to_string(), Value::Int64(Some(4))),
    ])
    .unwrap();

    db.flush().unwrap();

    // add field
    db.add_field("t1", "f3", DType::Int64, true).unwrap();

    // insert into memtable
    db.insert("t1", vec![
        NamedValue::new("f1".to_string(), Value::Int64(Some(5))),
        NamedValue::new("f2".to_string(), Value::Int64(Some(5))),
        NamedValue::new("f3".to_string(), Value::Int64(Some(5))),
    ])
    .unwrap();

    // scan should read from parquet as well as from memtable
    let stream = db
        .scan_partition("t1", 0, vec![
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
        ])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
            PrimitiveArray::<i64>::from(vec![None, None, None]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    let b = stream.next().await;
    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(4)]).boxed(),
            PrimitiveArray::<i64>::from(vec![Some(4)]).boxed(),
            PrimitiveArray::<i64>::from(vec![None]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    let b = stream.next().await;
    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
            PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
            PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
        ]),
        b.unwrap().unwrap()
    );

    // add values that intersect
    db.insert("t1", vec![
        NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
        NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
        NamedValue::new("f3".to_string(), Value::Int64(Some(1))),
    ])
    .unwrap();

    // scan should read from parquet as well as from memtable
    let stream = db
        .scan_partition("t1", 0, vec![
            "f1".to_string(),
            "f2".to_string(),
            "f3".to_string(),
        ])
        .unwrap();

    let mut stream: Pin<
        Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>,
    > = Box::pin(stream);
    let b = stream.next().await;

    assert_eq!(
        Chunk::new(vec![
            PrimitiveArray::<i64>::from(vec![Some(1), Some(1), Some(2), Some(3), Some(4), Some(5)])
                .boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(1), Some(2), None, Some(4), Some(5)])
                .boxed(),
            PrimitiveArray::<i64>::from(vec![None, Some(1), None, None, None, Some(5)]).boxed(),
        ]),
        b.unwrap().unwrap()
    );
}
