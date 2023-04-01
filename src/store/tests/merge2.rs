use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::path::Path;

use arrow2::array::Array;
use arrow2::array::BooleanArray;
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
use arrow2::io::print;
use arrow2::offset::Offset;
use arrow2::types::NativeType;
use parquet2::compression::CompressionOptions;
use parquet2::page::CompressedPage;
use parquet2::read::{get_page_iterator, read_metadata};
use parquet2::schema::types::ParquetType;
use parquet2::write::FileSeqWriter;
use parquet2::write::Version;
use parquet2::write::WriteOptions;
use store::error::Result;
use store::parquet_new::merger::FileMerger;
use store::parquet_new::parquet::CompressedPageIterator;
use store::test_util::create_parquet_from_arrays;
use store::test_util::parse_markdown_table;

#[test]
fn test_merger() -> anyhow::Result<()> {
    let iter1 = {
        let data = r#"
| a | b | c | f |
|---|---|---|---|
| 1 | 1 | 1 |   |
| 1 | 2 | 2 | 1 |
| 1 | 3 | 3 |   |
| 2 | 1 |   | 2 |
| 3 | 1 | 1 |   |
| 3 | 2 | 2 | 3 |
| 3 | 3 | 3 |   |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("f", DataType::Int64, true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        println!("{:?}", parsed);
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p1.parquet",
            fields.clone(),
            2,
            2,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p1.parquet")?)?
    };

    let iter2 = {
        let data = r#"
| a | b | c | d     |
|---|---|---|-------|
| 1 | 1 | 1 | 1,2,3 |
| 1 | 2 | 2 |       |
| 1 | 3 | 3 | 1     |
| 2 | 1 |   | 1,2   |
| 3 | 1 | 1 | 1     |
| 3 | 2 | 2 | 1,3   |
| 3 | 3 | 3 | 3     |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p2.parquet",
            fields.clone(),
            2,
            2,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p2.parquet")?)?
    };

    let iter3 = {
        let data = r#"
| a | b | c | d   | e   |
|---|---|---|-----|-----|
| 1 | 1 | 1 |     | 1   |
| 1 | 2 | 2 | 1,2 | 2   |
| 1 | 3 | 3 | 2   | 3,4 |
| 2 | 1 |   | 3   | 1   |
| 3 | 1 | 1 | 4   | 2   |
| 3 | 2 | 2 | 4,1 | 3,4 |
| 3 | 3 | 3 |     |     |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
            Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p3.parquet",
            fields.clone(),
            2,
            2,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p3.parquet")?)?
    };

    let w = File::create("/tmp/optiprism/merged.parquet")?;
    let mut merger = FileMerger::try_new(
        vec![iter1, iter2, iter3],
        w,
        2,
        2,
        2,
        2,
        "merged".to_string(),
    )?;
    merger.merge()?;

    let mut reader = File::open("/tmp/optiprism/p3.parquet")?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;


    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(reader, metadata.row_groups, schema.clone(), Some(1024 * 8 * 8), None, None);

    let chunks = chunks.into_iter().map(|c| c.unwrap()).collect::<Vec<_>>();
    let names = schema.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    println!("{}", print::write(&chunks, &names));
    Ok(())
}


#[test]
fn test_merger2() -> anyhow::Result<()> {
    let iter1 = {
        let data = r#"
| a | b | c     | d | e           |
|---|---|-------|---|-------------|
| 1 | 1 | 1,2,3 | a | a,b         |
| 1 | 2 |       | b | c           |
| 1 | 3 | 1     | c | e           |
| 2 | 1 | 1,2   | d | f,e         |
| 3 | 1 | 1     | e |             |
| 3 | 2 | 1,3   |   | e,f         |
| 3 | 3 | 3     | g | qwe qwe ыва |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
            Field::new("d", DataType::Utf8, true),
            Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Utf8, true))), true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        println!("{:?}", parsed);
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p1.parquet",
            fields.clone(),
            1,
            1,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p1.parquet")?)?
    };

    let iter2 = {
        let data = r#"
| a | b | c     |
|---|---|-------|
| 1 | 1 | 1,2,3 |
| 1 | 2 |       |
| 1 | 3 | 1     |
| 2 | 1 | 1,2   |
| 3 | 1 | 1     |
| 3 | 2 | 1,3   |
| 3 | 3 | 3     |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p2.parquet",
            fields.clone(),
            2,
            2,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p2.parquet")?)?
    };

    let iter3 = {
        let data = r#"
| a | b | c     |
|---|---|-------|
| 1 | 1 | 1,2,3 |
| 1 | 2 |       |
| 1 | 3 | 1     |
| 2 | 1 | 1,2   |
| 3 | 1 | 1     |
| 3 | 2 | 1,3   |
| 3 | 3 | 3     |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p3.parquet",
            fields.clone(),
            5,
            1,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p3.parquet")?)?
    };

    let w = File::create("/tmp/optiprism/merged.parquet")?;
    let mut merger = FileMerger::try_new(
        vec![iter1, iter2, iter3],
        w,
        2,
        2,
        2,
        2,
        "merged".to_string(),
    )?;
    merger.merge()?;

    let mut reader = File::open("/tmp/optiprism/merged.parquet")?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;


    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(reader, metadata.row_groups, schema.clone(), Some(1024 * 8 * 8), None, None);

    let chunks = chunks.into_iter().map(|c| c.unwrap()).collect::<Vec<_>>();
    let names = schema.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    println!("{}", print::write(&chunks, &names));
    Ok(())
}

#[test]
fn test_merger3() -> anyhow::Result<()> {
    let iter1 = {
        let data = r#"
| a | b | c     |
|---|---|-------|
| 1 | 1 | 1     |
| 2 | 2 |       |
| 2 | 2 |       |
| 2 | 2 | 2      |
    "#;

        let fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ];

        let parsed = parse_markdown_table(data, &fields)?;
        println!("{:?}", parsed);
        create_parquet_from_arrays(
            parsed.clone(),
            "/tmp/optiprism/p1.parquet",
            fields.clone(),
            2,
            2,
        )?;

        CompressedPageIterator::try_new(File::open("/tmp/optiprism/p1.parquet")?)?
    };

    let iter2 = CompressedPageIterator::try_new(File::open("/tmp/optiprism/p1.parquet")?)?;

    let w = File::create("/tmp/optiprism/merged.parquet")?;
    let mut merger = FileMerger::try_new(
        vec![iter1, iter2],
        w,
        2,
        2,
        2,
        2,
        "merged".to_string(),
    )?;
    merger.merge()?;

    let mut reader = File::open("/tmp/optiprism/merged.parquet")?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;


    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(reader, metadata.row_groups, schema.clone(), Some(1024 * 8 * 8), None, None);

    let chunks = chunks.into_iter().map(|c| c.unwrap()).collect::<Vec<_>>();
    let names = schema.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();
    println!("{}", print::write(&chunks, &names));
    Ok(())
}

#[test]
fn test_merger4() -> anyhow::Result<()> {
    let data = r#"
| a | b | c     |
|---|---|-------|
| 1 | 1 | 1,2,3 |
| 1 | 2 |       |
| 1 | 3 | 1     |
| 2 | 1 | 1,2   |
| 3 | 1 | 1     |
| 3 | 2 | 1,3   |
| 3 | 3 | 3     |
    "#;

    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::List(Box::new(Field::new("1", DataType::Int64, true))), true),
    ];

    let parsed = parse_markdown_table(data, &fields)?;
    create_parquet_from_arrays(
        parsed.clone(),
        "/tmp/optiprism/p3.parquet",
        fields.clone(),
        2,
        2,
    )?;

    let path = "/tmp/optiprism/p2.parquet";
    CompressedPageIterator::try_new(File::open(path)?)?;

    let mut reader = std::fs::File::open(path)?;
    let metadata = read_metadata(&mut reader)?;
    for row_group in metadata.row_groups.iter() {
        println!("rg");
        for column in row_group.columns().iter() {
            println!("col");
            for page in get_page_iterator(column, &mut reader, None, vec![], 1024 * 1024)? {
                let page = page?;
                println!("{:?}", page);
            }
        }
    }

    Ok(())
}

#[test]
fn test_merger5() -> anyhow::Result<()> {
    let data = r#"
| a | b | c     |
|---|---|-------|
| 1 | 1 | 1,2,3 |
| 1 | 2 |       |
| 1 | 3 | 1     |
| 2 | 1 | 1,2   |
| 3 | 1 | 1     |
| 3 | 2 | 1,3   |
| 3 | 3 | 3     |
    "#;

    let fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ];

    let n = 100;
    let f = 3;

    let mut out: Vec<Vec<i64>> = vec![vec![]; 3];

    let mut c = 0;
    for idx in 1..n {
        for v in 1..idx {
            out[0].push(idx);
            out[1].push(v);
            out[2].push(c);
            c += 1;
        }
    }

    println!("{:?}", out);

    Ok(())
}