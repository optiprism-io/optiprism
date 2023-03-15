#![feature(slice_take)]

// pub mod dictionary;
pub mod error;
pub mod arrow_conversion;
pub mod parquet;
// mod table;
// mod iterator;
// mod parquet;
// mod parquet2;

// pub mod schema;
use error::Result;

pub mod test_util {
    use arrow2::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray, MutableBooleanArray, MutableListArray, MutablePrimitiveArray, MutableUtf8Array, Utf8Array};
    use arrow2::datatypes::{DataType, Field};
    use arrow2::types::NativeType;
    use arrow2::array::{TryExtend};

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
                _ => unimplemented!("{:?}", data_type)
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
            let data = data.trim();
            let val = match data.is_empty() {
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

    pub fn create_list_primitive_array<N: NativeType, U: AsRef<[N]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
        });
        let mut array = MutableListArray::<i32, MutablePrimitiveArray<N>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_bool_array<U: AsRef<[bool]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
        });
        let mut array = MutableListArray::<i32, MutableBooleanArray>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_string_array<U: AsRef<[String]>, T: AsRef<[Option<U>]>>(data: T) -> ListArray<i32> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(x.to_owned())).collect::<Vec<_>>())
        });
        let mut array = MutableListArray::<i32, MutableUtf8Array<i32>>::new();
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
    /// +---+-------+------+-------+-------+
    /// | a |     b |    c |     d | e     |
    /// +---+-------+------+-------+-------+
    /// | 1 |  true | test | 1,2,3 | a,b,c |
    /// | 2 |       |      |   1,2 | b     |
    /// | 3 | false | lala |       |       |
    /// +---+-------+------+-------+-------+
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
    ///
    pub fn parse_markdown_table(data: &str, fields: &[Field]) -> anyhow::Result<Vec<Box<dyn Array>>> {
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
}