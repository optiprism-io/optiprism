#![feature(slice_take)]
#![feature(let_chains)]

extern crate core;

// pub mod dictionary;
pub mod arrow_conversion;
pub mod error;
pub mod parquet;
pub mod parquet_new;
// mod table;
// mod iterator;
// mod parquet;

// pub mod schema;
use error::Result;

pub mod test_util {
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    use anyhow::anyhow;
    use arrow2::array::{Array, BinaryArray, FixedSizeBinaryArray, MutableBinaryArray, MutableFixedSizeBinaryArray, PrimitiveArray};
    use arrow2::array::BooleanArray;
    use arrow2::array::Float64Array;
    use arrow2::array::Int32Array;
    use arrow2::array::Int64Array;
    use arrow2::array::ListArray;
    use arrow2::array::MutableBooleanArray;
    use arrow2::array::MutableListArray;
    use arrow2::array::MutablePrimitiveArray;
    use arrow2::array::MutableUtf8Array;
    use arrow2::array::TryExtend;
    use arrow2::array::Utf8Array;
    use arrow2::chunk::Chunk;
    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::PhysicalType;
    use arrow2::datatypes::Schema;
    use arrow2::io::parquet::write::array_to_page_nested;
    use arrow2::io::parquet::write::array_to_page_simple;
    use arrow2::io::parquet::write::to_parquet_schema;
    use arrow2::io::parquet::write::transverse;
    use arrow2::io::parquet::write::FileWriter;
    use arrow2::io::parquet::write::RowGroupIterator;
    use arrow2::io::parquet::write::WriteOptions;
    use arrow2::offset::Offset;
    use arrow2::types::NativeType;
    use parquet2::compression::CompressionOptions;
    use parquet2::encoding::Encoding;
    use parquet2::schema::types::PrimitiveType;
    use parquet2::write::FileSeqWriter;
    use parquet2::write::Version;


    #[derive(Debug, Clone)]
    pub enum ListValue {
        String(String),
        Int32(i32),
        Int64(i64),
        Float(f64),
        Bool(bool),
    }

    impl ListValue {
        pub fn parse(data: &str, data_type: &DataType) -> anyhow::Result<Self> {
            let data = data.trim();
            if data.is_empty() {
                return Err(anyhow::Error::msg("empty value"));
            }

            Ok(match data_type {
                DataType::Int64 => ListValue::Int64(data.parse()?),
                DataType::Int32 => ListValue::Int32(data.parse()?),
                DataType::Float64 => ListValue::Float(data.parse()?),
                DataType::Boolean => ListValue::Bool(data.parse()?),
                DataType::Utf8 => ListValue::String(data.parse()?),
                _ => unimplemented!("{:?}", data_type),
            })
        }
    }

    impl Into<i64> for ListValue {
        fn into(self) -> i64 {
            match self {
                ListValue::Int64(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl Into<i32> for ListValue {
        fn into(self) -> i32 {
            match self {
                ListValue::Int32(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl Into<f64> for ListValue {
        fn into(self) -> f64 {
            match self {
                ListValue::Float(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl Into<bool> for ListValue {
        fn into(self) -> bool {
            match self {
                ListValue::Bool(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl Into<String> for ListValue {
        fn into(self) -> String {
            match self {
                ListValue::String(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    #[derive(Debug, Clone)]
    pub enum Value {
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
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<i64> {
        fn from(value: Value) -> Self {
            match value {
                Value::Int64(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<f64> {
        fn from(value: Value) -> Self {
            match value {
                Value::Float(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<bool> {
        fn from(value: Value) -> Self {
            match value {
                Value::Bool(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<String> {
        fn from(value: Value) -> Self {
            match value {
                Value::String(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<Vec<ListValue>> {
        fn from(value: Value) -> Self {
            match value {
                Value::List(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl Into<Option<Vec<i64>>> for Value {
        fn into(self) -> Option<Vec<i64>> {
            match self {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl Into<Option<Vec<i32>>> for Value {
        fn into(self) -> Option<Vec<i32>> {
            match self {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl Into<Option<Vec<f64>>> for Value {
        fn into(self) -> Option<Vec<f64>> {
            match self {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl Into<Option<Vec<String>>> for Value {
        fn into(self) -> Option<Vec<String>> {
            match self {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl Into<Option<Vec<bool>>> for Value {
        fn into(self) -> Option<Vec<bool>> {
            match self {
                Value::List(v) => v.map(|v| {
                    v.into_iter()
                        .map(|v| match v {
                            ListValue::Bool(v) => v,
                            _ => unimplemented!(),
                        })
                        .collect()
                }),
                _ => unimplemented!(),
            }
        }
    }

    impl Value {
        pub fn parse(data: &str, data_type: &DataType) -> anyhow::Result<Self> {
            let data = data.trim();
            let val = match data.is_empty() {
                true => match data_type {
                    DataType::Int64 => Value::Int64(None),
                    DataType::Int32 => Value::Int32(None),
                    DataType::Float64 => Value::Float(None),
                    DataType::Boolean => Value::Bool(None),
                    DataType::Utf8 => Value::String(None),
                    DataType::List(_) => Value::List(None),
                    _ => unimplemented!(),
                },
                false => match data_type {
                    DataType::Int64 => Value::Int64(Some(data.parse()?)),
                    DataType::Int32 => Value::Int32(Some(data.parse()?)),
                    DataType::Float64 => Value::Float(Some(data.parse()?)),
                    DataType::Boolean => Value::Bool(Some(data.parse()?)),
                    DataType::Utf8 => Value::String(Some(data.parse()?)),
                    _ => unimplemented!(),
                },
            };

            Ok(val)
        }
    }

    pub fn gen_idx_primitive_array<T: NativeType + num_traits::NumCast>(n: usize) -> PrimitiveArray<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            for _ in 0..idx {
                ret.push(T::from(idx).unwrap());
            }
        }

        PrimitiveArray::<T>::from_slice(ret)
    }

    pub fn gen_secondary_idx_primitive_array<T: NativeType + num_traits::NumCast>(
        n: usize,
    ) -> PrimitiveArray<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            for v in 1..idx {
                ret.push(T::from(v).unwrap());
            }
        }

        PrimitiveArray::<T>::from_slice(ret)
    }

    pub fn gen_primitive_data_array<T: NativeType + num_traits::NumCast>(
        n: usize,
        nulls: Option<usize>,
    ) -> PrimitiveArray<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(T::from(idx).unwrap()));
            }
        }

        PrimitiveArray::<T>::from(ret)
    }


    pub fn gen_utf8_data_array<T: Offset>(n: usize, nulls: Option<usize>) -> Utf8Array<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(format!("{idx}")));
            }
        }

        Utf8Array::<T>::from(ret)
    }

    pub fn gen_binary_data_array<T: Offset>(n: usize, nulls: Option<usize>) -> BinaryArray<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(format!("{idx}")));
            }
        }

        BinaryArray::<T>::from(ret)
    }


    pub fn gen_fixed_size_binary_data_array(
        n: usize,
        nulls: Option<usize>,
        size: usize,
    ) -> FixedSizeBinaryArray {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(format!("{:0size$}", idx, size = size)));
            }
        }

        FixedSizeBinaryArray::from_iter(ret, size)
    }

    pub fn gen_boolean_data_array(n: usize, nulls: Option<usize>) -> BooleanArray {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(idx % 2 == 0));
            }
        }

        BooleanArray::from(ret)
    }

    pub fn gen_primitive_data_list_array<O: Offset, N: NativeType + num_traits::NumCast>(n: usize, nulls: Option<usize>) -> ListArray<O> {
        let mut vals = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                vals.push(None);
            } else {
                vals.push(Some(vec![N::from(idx).unwrap()]));
            }
        }

        create_list_primitive_array::<O, N, _, _>(vals)
    }

    pub fn gen_utf8_data_list_array<O: Offset>(n: usize, nulls: Option<usize>) -> ListArray<O> {
        let mut vals = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                vals.push(None);
            } else {
                vals.push(Some(vec![format!("{idx}")]));
            }
        }

        create_list_utf8_array::<O, _, _>(vals)
    }

    pub fn gen_binary_data_list_array<O: Offset>(n: usize, nulls: Option<usize>) -> ListArray<O> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(vec![format!("{idx}")]));
            }
        }

        create_list_binary_array::<O, _, _>(ret)
    }

    pub fn gen_boolean_data_list_array<O: Offset>(n: usize, nulls: Option<usize>) -> ListArray<O> {
        let mut vals = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                vals.push(None);
            } else {
                vals.push(Some(vec![idx % 2 == 0]));
            }
        }

        create_list_bool_array::<O, _, _>(vals)
    }

    pub fn create_list_primitive_array<O: Offset, N: NativeType, U: AsRef<[N]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ListArray<O> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
        });
        let mut array = MutableListArray::<O, MutablePrimitiveArray<N>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_bool_array<O: Offset, U: AsRef<[bool]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ListArray<O> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
        });
        let mut array = MutableListArray::<O, MutableBooleanArray>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_utf8_array<O: Offset, U: AsRef<[String]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ListArray<O> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref().map(|x| {
                x.as_ref()
                    .iter()
                    .map(|x| Some(x.to_owned()))
                    .collect::<Vec<_>>()
            })
        });
        let mut array = MutableListArray::<O, MutableUtf8Array<i32>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_binary_array<O: Offset, U: AsRef<[String]>, T: AsRef<[Option<U>]>>(
        data: T,
    ) -> ListArray<O> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref().map(|x| {
                x.as_ref()
                    .iter()
                    .map(|x| Some(x.to_owned()))
                    .collect::<Vec<_>>()
            })
        });
        let mut array = MutableListArray::<O, MutableBinaryArray<i32>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    /// Parses a markdown table into a vector of arrays:
    ///  * Types supported: Int64, Int32, Float64, Boolean, Utf8, List
    ///  * List types supported: Int64, Int32, Float64, Boolean, Utf8
    ///  * List should be only one level deep
    ///
    /// # Arguments
    /// * `data`   - The markdown table. The first row must be the header. You can use a list of values
    ///              separated by commas to create a list array
    /// * `fields` - The fields of the table
    ///
    /// # Example
    ///     let data = r#"
    /// ```markdown
    /// | a | b     | c    | d     | e     |
    /// |---|-------|------|-------|-------|
    /// | 1 | true  | test | 1,2,3 | a,b,c |
    /// | 2 |       |      | 1,2   | b     |
    /// | 3 | false | lala |       |       |
    /// ```
    ///     "#;
    ///     let fields = vec![
    ///         Field::new("a", DataType::Int64, true),
    ///         Field::new("b", DataType::Boolean, true),
    ///         Field::new("c", DataType::Utf8, true),
    ///         Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int32, true))), true),
    ///         Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Utf8, true))), true),
    ///     ];
    ///
    ///     let parsed = parse_markdown_table(data.to_string(), &fields)?;
    ///     println!("{:#?}", parsed);
    ///
    ///    // Output:
    ///    // [
    ///    //  Int64[1, 2, 3],
    ///    //  BooleanArray[true, None, false],
    ///    //  LargeUtf8Array[test, None, lala],
    ///    //  ListArray[[1, 2, 3], [1, 2], None],
    ///    //  ListArray[[a, b, c], [b], None],
    ///    // ]
    pub fn parse_markdown_table(
        data: &str,
        fields: &[Field],
    ) -> anyhow::Result<Vec<Box<dyn Array>>> {
        let mut out: Vec<Vec<Value>> = vec![vec![]; fields.len()];
        for row in data.lines().skip(3) {
            let v = row
                .split('|')
                .skip(1)
                .take(fields.len())
                .collect::<Vec<_>>();
            // skip non-data lines
            if v.len() != fields.len() {
                continue;
            }

            for ((idx, val), field) in v.into_iter().enumerate().zip(fields.iter()) {
                match field.data_type() {
                    DataType::Int64
                    | DataType::Int32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Utf8 => out[idx].push(Value::parse(val, field.data_type())?),
                    DataType::List(f) | DataType::LargeList(f) => {
                        if val.trim().is_empty() {
                            out[idx].push(Value::List(None));
                            continue;
                        }
                        let split = val.trim().split(',');
                        let vals = split
                            .map(|v| ListValue::parse(v, f.data_type()).unwrap())
                            .collect();
                        out[idx].push(Value::List(Some(vals)));
                    }
                    _ => unimplemented!(),
                }
            }
        }

        let result = out
            .into_iter()
            .zip(fields.iter())
            .map(|(vals, field)| match field.data_type() {
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
                    let vals: Vec<Option<String>> =
                        vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                    Utf8Array::<i64>::from(vals).boxed()
                }
                DataType::List(f) => match f.data_type {
                    DataType::Int64 => {
                        let vals: Vec<Option<Vec<i64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals).boxed()
                    }
                    DataType::Int32 => {
                        let vals: Vec<Option<Vec<i32>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals).boxed()
                    }
                    DataType::Float64 => {
                        let vals: Vec<Option<Vec<f64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals).boxed()
                    }
                    DataType::Boolean => {
                        let vals: Vec<Option<Vec<bool>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_bool_array::<i32, _, _>(vals).boxed()
                    }
                    DataType::Utf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i32, _, _>(vals).boxed()
                    }
                    _ => unimplemented!(),
                },
                DataType::LargeList(f) => match f.data_type {
                    DataType::Int64 => {
                        let vals: Vec<Option<Vec<i64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals).boxed()
                    }
                    DataType::Int32 => {
                        let vals: Vec<Option<Vec<i32>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals).boxed()
                    }
                    DataType::Float64 => {
                        let vals: Vec<Option<Vec<f64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals).boxed()
                    }
                    DataType::Boolean => {
                        let vals: Vec<Option<Vec<bool>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_bool_array::<i64, _, _>(vals).boxed()
                    }
                    DataType::Utf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i64, _, _>(vals).boxed()
                    }
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    pub fn create_parquet_from_arrays(
        mut arrs: Vec<Box<dyn Array>>,
        path: impl AsRef<Path>,
        fields: Vec<Field>,
        page_size: usize,
        pages_per_row_group: usize,
    ) -> anyhow::Result<()> {
        let schema = Schema::from(fields);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Snappy,
            version: Version::V2,
            data_pagesize_limit: Some(20),
        };

        let mut idx = 0;
        let mut chunks = vec![];
        while idx < arrs[0].len() {
            println!("idx {idx} pages_per_row_group {pages_per_row_group} page_size {page_size} len {}", arrs[0].len());
            let end = std::cmp::min(idx + (pages_per_row_group * page_size), arrs[0].len());
            let chunk = arrs.iter().map(|arr| arr.sliced(idx, end - idx)).collect::<Vec<_>>();
            println!("{:?}", chunk);
            chunks.push(Ok(Chunk::new(chunk)));
            idx += pages_per_row_group * page_size;
        }

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups =
            RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;

        // Create a new empty file
        let file = File::create(path)?;

        let mut writer = FileWriter::try_new(file, schema, options)?;

        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;

        Ok(())
    }
}
