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

macro_rules! make_arrays {
    ($type:tt $(, $page:tt)*) => {{
        vec!($(vec!$page),*).into_iter().map(|vals| $type::from_slice(vals).boxed()).collect::<Vec<_>>()
    }};
}

macro_rules! make_nullable_arrays {
    ($type:ident $(, $page:tt)*) => {{
        vec!($(vec!$page),*).iter().map(|vals| $type::from(vals).boxed()).collect::<Vec<_>>()
    }};
}

macro_rules! make_utf_arrays {
    ($($page:tt),*) => {{
        vec!($(vec!$page),*).into_iter().map(|vals| Utf8Array::<i64>::from_slice(vals).boxed()).collect::<Vec<_>>()
    }};
}

macro_rules! make_nullable_utf_arrays {
    ($($page:tt),*) => {{
        vec!($(vec!$page),*).into_iter().map(|vals| Utf8Array::<i64>::from(vals).boxed()).collect::<Vec<_>>()
    }};
}

#[test]
fn test2() -> anyhow::Result<()> {
    let data = r#"city,lat,lng
"Elgin, Scotland, the UK",57.653484,-3.335724
"Stoke-on-Trent, Staffordshire, the UK",53.002666,-2.179404
"Solihull, Birmingham, UK",52.412811,-1.778197
"Cardiff, Cardiff county, UK",51.481583,-3.179090
"Eastbourne, East Sussex, UK",50.768036,0.290472
"Oxford, Oxfordshire, UK",51.752022,-1.257677
"London, UK",51.509865,-0.118092
"Swindon, Swindon, UK",51.568535,-1.772232
"Gravesend, Kent, UK",51.441883,0.370759
"Northampton, Northamptonshire, UK",52.240479,-0.902656
"Rugby, Warwickshire, UK",52.370876,-1.265032
"Sutton Coldfield, West Midlands, UK",52.570385,-1.824042
"Harlow, Essex, UK",51.772938,0.102310
"Aberdeen, Aberdeen City, UK",57.149651,-2.099075"#;
    let mut reader = ReaderBuilder::new().from_reader(Cursor::new(data));
    let (fields, _) = infer_schema(&mut reader, None, true, &infer)?;

    let mut rows = vec![ByteRecord::default(); 100];
    let rows_read = read_rows(&mut reader, 0, &mut rows)?;

    let columns = deserialize_batch(
        &rows[..rows_read],
        &fields,
        None,
        0,
        deserialize_column,
    )?;

    assert_eq!(14, columns.len());
    assert_eq!(3, columns.arrays().len());

    println!("{:#?}", columns);
    Ok(())
}

#[derive(Debug, Clone)]
enum ListValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Float(f64),
    Bool(bool),
}

impl ListValue {
    pub fn parse(data: &str, data_type: &DataType) -> anyhow::Result<Self> {
        if data.trim().is_empty() {
            return Err(anyhow::Error::msg("empty value"));
        }

        Ok(match data_type {
            DataType::Int64 => ListValue::Int64(data.parse()?),
            DataType::Int32 => ListValue::Int32(data.parse()?),
            DataType::Float64 => ListValue::Float(data.parse()?),
            DataType::Boolean => ListValue::Bool(data.parse()?),
            DataType::Utf8 => ListValue::String(data.parse()?),
            _ => unimplemented!()
        })
    }
}

impl Into<i64> for ListValue {
    fn into(self) -> i64 {
        match self {
            ListValue::Int64(v) => v,
            _ => unimplemented!()
        }
    }
}

impl Into<i32> for ListValue {
    fn into(self) -> i32 {
        match self {
            ListValue::Int32(v) => v,
            _ => unimplemented!()
        }
    }
}

impl Into<f64> for ListValue {
    fn into(self) -> f64 {
        match self {
            ListValue::Float(v) => v,
            _ => unimplemented!()
        }
    }
}

impl Into<bool> for ListValue {
    fn into(self) -> bool {
        match self {
            ListValue::Bool(v) => v,
            _ => unimplemented!()
        }
    }
}

impl Into<String> for ListValue {
    fn into(self) -> String {
        match self {
            ListValue::String(v) => v,
            _ => unimplemented!()
        }
    }
}

#[derive(Debug, Clone)]
enum Value {
    String(Option<String>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float(Option<f64>),
    Bool(Option<bool>),
    List(Option<Vec<ListValue>>),
}

impl From<Value> for Option<i32> {
    fn from(value: Value) -> Self {
        match value {
            Value::Int32(v) => v,
            _ => unimplemented!()
        }
    }
}

impl From<Value> for Option<i64> {
    fn from(value: Value) -> Self {
        match value {
            Value::Int64(v) => v,
            _ => unimplemented!()
        }
    }
}

impl From<Value> for Option<f64> {
    fn from(value: Value) -> Self {
        match value {
            Value::Float(v) => v,
            _ => unimplemented!()
        }
    }
}

impl From<Value> for Option<bool> {
    fn from(value: Value) -> Self {
        match value {
            Value::Bool(v) => v,
            _ => unimplemented!()
        }
    }
}

impl From<Value> for Option<String> {
    fn from(value: Value) -> Self {
        match value {
            Value::String(v) => v,
            _ => unimplemented!()
        }
    }
}

impl From<Value> for Option<Vec<ListValue>> {
    fn from(value: Value) -> Self {
        match value {
            Value::List(v) => v,
            _ => unimplemented!()
        }
    }
}


impl Into<Option<Vec<i64>>> for Value {
    fn into(self) -> Option<Vec<i64>> {
        match self {
            Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
            _ => unimplemented!()
        }
    }
}

impl Into<Option<Vec<i32>>> for Value {
    fn into(self) -> Option<Vec<i32>> {
        match self {
            Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
            _ => unimplemented!()
        }
    }
}

impl Into<Option<Vec<f64>>> for Value {
    fn into(self) -> Option<Vec<f64>> {
        match self {
            Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
            _ => unimplemented!()
        }
    }
}

impl Into<Option<Vec<String>>> for Value {
    fn into(self) -> Option<Vec<String>> {
        match self {
            Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
            _ => unimplemented!()
        }
    }
}

impl Into<Option<Vec<bool>>> for Value {
    fn into(self) -> Option<Vec<bool>> {
        match self {
            Value::List(v) => v.map(|v| v.into_iter().map(|v| match v {
                ListValue::Bool(v) => v,
                _ => unimplemented!()
            }).collect()),
            _ => unimplemented!()
        }
    }
}

impl Value {
    pub fn parse(data: &str, data_type: &DataType) -> anyhow::Result<Self> {
        let val = match data.trim().is_empty() {
            true => match data_type {
                DataType::Int64 => Value::Int64(None),
                DataType::Int32 => Value::Int32(None),
                DataType::Float64 => Value::Float(None),
                DataType::Boolean => Value::Bool(None),
                DataType::Utf8 => Value::String(None),
                DataType::List(_) => Value::List(None),
                _ => unimplemented!()
            }
            false => match data_type {
                DataType::Int64 => Value::Int64(Some(data.parse()?)),
                DataType::Int32 => Value::Int32(Some(data.parse()?)),
                DataType::Float64 => Value::Float(Some(data.parse()?)),
                DataType::Boolean => Value::Bool(Some(data.parse()?)),
                DataType::Utf8 => Value::String(Some(data.parse()?)),
                _ => unimplemented!()
            },
        };

        Ok(val)
    }
}

fn create_list_primitive_array<N: NativeType, U: AsRef<[N]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
    let iter = data.as_ref().iter().map(|x| {
        x.as_ref()
            .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
    });
    let mut array = MutableListArray::<i32, MutablePrimitiveArray<N>>::new();
    array.try_extend(iter).unwrap();
    array.into()
}

fn create_list_bool_array<U: AsRef<[bool]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
    let iter = data.as_ref().iter().map(|x| {
        x.as_ref()
            .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
    });
    let mut array = MutableListArray::<i32, MutableBooleanArray>::new();
    array.try_extend(iter).unwrap();
    array.into()
}

fn create_list_string_array<U: AsRef<[String]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
    let iter = data.as_ref().iter().map(|x| {
        x.as_ref()
            .map(|x| x.as_ref().iter().map(|x| Some(x.to_owned())).collect::<Vec<_>>())
    });
    let mut array = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
    array.try_extend(iter).unwrap();
    array.into()
}

fn parse(data: String, fields: &[Field]) -> anyhow::Result<Vec<Box<dyn Array>>> {
    let mut out: Vec<Vec<Value>> = vec![vec![]; fields.len()];
    for row in data.lines().skip(4) {
        let v = row.split('|').skip(1).collect::<Vec<_>>();
        for ((idx, val), field) in v.into_iter().take(fields.len()).enumerate().zip(fields.iter()) {
            match field.data_type() {
                DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Boolean | DataType::Utf8 => out[idx].push(Value::parse(val, field.data_type())?),
                DataType::List(f) => {
                    if val.trim().is_empty() {
                        out[idx].push(Value::List(None));
                        continue;
                    }
                    let split = val.trim().split(',');
                    let vals = split.map(|v| ListValue::parse(v, f.data_type()).unwrap()).collect();
                    out[idx].push(Value::List(Some(vals)));
                }
                _ => unimplemented!()
            }
        }
    }

    let result = out.into_iter().zip(fields.iter()).map(|(vals, field)| {
        match field.data_type() {
            DataType::Int64 => {
                let vals = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                Int64Array::from(vals).boxed()
            }
            DataType::Int32 => {
                let vals = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                Int32Array::from(vals).boxed()
            }
            DataType::Float64 => {
                let vals = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                Float64Array::from(vals).boxed()
            }
            DataType::Boolean => {
                let vals = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                BooleanArray::from(vals).boxed()
            }
            DataType::Utf8 => {
                let vals: Vec<Option<String>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                Utf8Array::<i64>::from(vals).boxed()
            }
            DataType::List(f) => {
                match f.data_type {
                    DataType::Int64 => {
                        let vals: Vec<Option<Vec<i64>>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array(vals).boxed()
                    }
                    DataType::Int32 => {
                        let vals: Vec<Option<Vec<i32>>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array(vals).boxed()
                    }
                    DataType::Float64 => {
                        let vals: Vec<Option<Vec<f64>>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array(vals).boxed()
                    }
                    DataType::Boolean => {
                        let vals: Vec<Option<Vec<bool>>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_bool_array(vals).boxed()
                    }
                    DataType::Utf8 => {
                        let vals: Vec<Option<Vec<String>>> = vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_string_array(vals).boxed()
                    }
                    _ => unimplemented!()
                }
            }
            _ => unimplemented!()
        }
    }).collect::<Vec<_>>();

    Ok(result)
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
        Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int8, true))), true),
        Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Utf8, true))), true),
    ];

    let parsed = parse(data.to_string(), &fields)?;

    println!("{:#?}", parsed);
    // create_parquet_file("/tmp/optiprism/test.parquet", data, vec!["a", "b", "c"])?;

    Ok(())
}
