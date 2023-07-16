#![feature(slice_take)]
#![feature(let_chains)]

extern crate core;

pub mod arrow_conversion;
pub mod error;
pub mod parquet;

pub mod test_util {
    use std::fs::File;
    use std::io::Read;
    use std::io::Seek;
    use std::io::Write;
    use std::path::Path;

    use arrow2::array::Array;
    use arrow2::array::BinaryArray;
    use arrow2::array::BooleanArray;
    use arrow2::array::FixedSizeBinaryArray;
    use arrow2::array::Float64Array;
    use arrow2::array::Int32Array;
    use arrow2::array::Int64Array;
    use arrow2::array::ListArray;
    use arrow2::array::MutableBinaryArray;
    use arrow2::array::MutableBooleanArray;
    use arrow2::array::MutableListArray;
    use arrow2::array::MutablePrimitiveArray;
    use arrow2::array::MutableUtf8Array;
    use arrow2::array::PrimitiveArray;
    use arrow2::array::TryExtend;
    use arrow2::array::Utf8Array;
    use arrow2::chunk::Chunk;
    use arrow2::compute::concatenate::concatenate;
    use arrow2::compute::take::take;
    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::PhysicalType;
    use arrow2::datatypes::Schema;
    use arrow2::io::parquet::read;
    use arrow2::io::parquet::write::transverse;
    use arrow2::io::parquet::write::FileWriter;
    use arrow2::io::parquet::write::RowGroupIterator;
    use arrow2::io::parquet::write::WriteOptions;
    use arrow2::offset::Offset;
    use arrow2::types::NativeType;
    use parquet2::compression::CompressionOptions;
    use parquet2::encoding::Encoding;
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

    impl From<ListValue> for i64 {
        fn from(value: ListValue) -> Self {
            match value {
                ListValue::Int64(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<ListValue> for i32 {
        fn from(value: ListValue) -> Self {
            match value {
                ListValue::Int32(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<ListValue> for f64 {
        fn from(value: ListValue) -> Self {
            match value {
                ListValue::Float(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<ListValue> for bool {
        fn from(value: ListValue) -> Self {
            match value {
                ListValue::Bool(v) => v,
                _ => unimplemented!(),
            }
        }
    }

    impl From<ListValue> for String {
        fn from(value: ListValue) -> Self {
            match value {
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

    impl From<Value> for Option<Vec<i64>> {
        fn from(value: Value) -> Self {
            match value {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<Vec<i32>> {
        fn from(value: Value) -> Self {
            match value {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<Vec<f64>> {
        fn from(value: Value) -> Self {
            match value {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<Vec<String>> {
        fn from(value: Value) -> Self {
            match value {
                Value::List(v) => v.map(|v| v.into_iter().map(|v| v.into()).collect()),
                _ => unimplemented!(),
            }
        }
    }

    impl From<Value> for Option<Vec<bool>> {
        fn from(value: Value) -> Self {
            match value {
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

    #[derive(Clone)]
    pub enum PrimaryIndexType {
        Partitioned(usize),
        Sequential(usize),
    }

    pub fn gen_idx_primitive_array<T: NativeType + num_traits::NumCast>(
        _type: PrimaryIndexType,
    ) -> PrimitiveArray<T> {
        let res = match _type {
            PrimaryIndexType::Partitioned(n) => {
                let mut ret = Vec::with_capacity(n * (n - 1) / 2);

                for idx in 0..n {
                    if n == 1 {
                        ret.push(T::from(idx).unwrap());
                    } else {
                        for _ in 0..idx {
                            ret.push(T::from(idx).unwrap());
                        }
                    }
                }

                ret
            }
            PrimaryIndexType::Sequential(n) => {
                let mut ret = Vec::with_capacity(n);
                for idx in 0..n {
                    ret.push(T::from(idx).unwrap());
                }

                ret
            }
        };

        PrimitiveArray::<T>::from_slice(res)
    }

    pub fn gen_idx_primitive_array_from_arrow_type(
        pt: &arrow2::types::PrimitiveType,
        dt: DataType,
        _type: PrimaryIndexType,
    ) -> Box<dyn Array> {
        match pt {
            arrow2::types::PrimitiveType::Int8 => {
                gen_idx_primitive_array::<i8>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int16 => {
                gen_idx_primitive_array::<i16>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int32 => {
                gen_idx_primitive_array::<i32>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int64 => {
                gen_idx_primitive_array::<i64>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int128 => {
                gen_idx_primitive_array::<i128>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt8 => {
                gen_idx_primitive_array::<u8>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt16 => {
                gen_idx_primitive_array::<u16>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt32 => {
                gen_idx_primitive_array::<u32>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt64 => {
                gen_idx_primitive_array::<u64>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float32 => {
                gen_idx_primitive_array::<f32>(_type).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float64 => {
                gen_idx_primitive_array::<f64>(_type).to(dt).boxed()
            }
            _ => unimplemented!(),
        }
    }

    pub fn gen_secondary_idx_primitive_array<T: NativeType + num_traits::NumCast>(
        n: usize,
    ) -> PrimitiveArray<T> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            for v in 0..idx {
                ret.push(T::from(v).unwrap());
            }
        }

        PrimitiveArray::<T>::from_slice(ret)
    }

    pub fn gen_secondary_idx_primitive_array_from_arrow_type(
        pt: &arrow2::types::PrimitiveType,
        dt: DataType,
        n: usize,
    ) -> Box<dyn Array> {
        match pt {
            arrow2::types::PrimitiveType::Int8 => {
                gen_secondary_idx_primitive_array::<i8>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int16 => {
                gen_secondary_idx_primitive_array::<i16>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int32 => {
                gen_secondary_idx_primitive_array::<i32>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int64 => {
                gen_secondary_idx_primitive_array::<i64>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int128 => {
                gen_secondary_idx_primitive_array::<i128>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt8 => {
                gen_secondary_idx_primitive_array::<u8>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt16 => {
                gen_secondary_idx_primitive_array::<u16>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt32 => {
                gen_secondary_idx_primitive_array::<u32>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt64 => {
                gen_secondary_idx_primitive_array::<u64>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float32 => {
                gen_secondary_idx_primitive_array::<f32>(n).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float64 => {
                gen_secondary_idx_primitive_array::<f64>(n).to(dt).boxed()
            }
            _ => unimplemented!(),
        }
    }

    pub fn native_type_length<N: NativeType>() -> usize {
        match N::PRIMITIVE {
            arrow2::types::PrimitiveType::Int8 => i8::MAX as usize,
            arrow2::types::PrimitiveType::Int16 => i16::MAX as usize,
            arrow2::types::PrimitiveType::Int32 => i32::MAX as usize,
            arrow2::types::PrimitiveType::Int64 => i64::MAX as usize,
            arrow2::types::PrimitiveType::Int128 => i128::MAX as usize,
            arrow2::types::PrimitiveType::UInt8 => u8::MAX as usize,
            arrow2::types::PrimitiveType::UInt16 => u16::MAX as usize,
            arrow2::types::PrimitiveType::UInt32 => u32::MAX as usize,
            arrow2::types::PrimitiveType::UInt64 => u64::MAX as usize,
            arrow2::types::PrimitiveType::Float32 => f32::MAX as usize,
            arrow2::types::PrimitiveType::Float64 => f64::MAX as usize,
            _ => unreachable!(),
        }
    }

    pub fn gen_primitive_data_array<T: NativeType + num_traits::NumCast>(
        n: usize,
        nulls: Option<usize>,
    ) -> PrimitiveArray<T> {
        let mut ret = Vec::with_capacity(n);
        let div = native_type_length::<T>();
        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                let _a = std::mem::size_of::<T>();
                ret.push(Some(T::from(idx % div).unwrap()));
            }
        }

        PrimitiveArray::<T>::from(ret)
    }

    pub fn gen_primitive_data_array_from_arrow_type(
        pt: &arrow2::types::PrimitiveType,
        dt: DataType,
        n: usize,
        nulls: Option<usize>,
    ) -> Box<dyn Array> {
        match pt {
            arrow2::types::PrimitiveType::Int8 => {
                gen_primitive_data_array::<i8>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int16 => {
                gen_primitive_data_array::<i16>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int32 => {
                gen_primitive_data_array::<i32>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int64 => {
                gen_primitive_data_array::<i64>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Int128 => {
                gen_primitive_data_array::<i128>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt8 => {
                gen_primitive_data_array::<u8>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt16 => {
                gen_primitive_data_array::<u16>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt32 => {
                gen_primitive_data_array::<u32>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::UInt64 => {
                gen_primitive_data_array::<u64>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float32 => {
                gen_primitive_data_array::<f32>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::Float64 => {
                gen_primitive_data_array::<f64>(n, nulls).to(dt).boxed()
            }
            arrow2::types::PrimitiveType::DaysMs => {
                gen_primitive_data_array::<i64>(n, nulls).to(dt).boxed()
            }
            _ => unimplemented!("{:?}", pt),
        }
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

    pub fn gen_primitive_data_list_array<O: Offset, N: NativeType + num_traits::NumCast>(
        n: usize,
        dt: DataType,
        nulls: Option<usize>,
    ) -> ListArray<O> {
        let mut vals = Vec::with_capacity(n);
        let div = native_type_length::<N>();
        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                vals.push(None);
            } else {
                vals.push(Some(vec![N::from(idx % div).unwrap()]));
            }
        }

        create_list_primitive_array::<O, N, _, _>(vals, dt)
    }

    pub fn gen_primitive_data_list_array_from_arrow_type<O: Offset>(
        pt: &arrow2::types::PrimitiveType,
        dt: DataType,
        n: usize,
        nulls: Option<usize>,
    ) -> Box<dyn Array> {
        match pt {
            arrow2::types::PrimitiveType::Int8 => {
                gen_primitive_data_list_array::<O, i8>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Int16 => {
                gen_primitive_data_list_array::<O, i16>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Int32 => {
                gen_primitive_data_list_array::<O, i32>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Int64 => {
                gen_primitive_data_list_array::<O, i64>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Int128 => {
                gen_primitive_data_list_array::<O, i128>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::UInt8 => {
                gen_primitive_data_list_array::<O, u8>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::UInt16 => {
                gen_primitive_data_list_array::<O, u16>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::UInt32 => {
                gen_primitive_data_list_array::<O, u32>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::UInt64 => {
                gen_primitive_data_list_array::<O, u64>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Float32 => {
                gen_primitive_data_list_array::<O, f32>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::Float64 => {
                gen_primitive_data_list_array::<O, f64>(n, dt, nulls)
            }
            arrow2::types::PrimitiveType::DaysMs => {
                gen_primitive_data_list_array::<O, i64>(n, dt, nulls)
            }
            _ => unimplemented!("{:?}", pt),
        }
        .boxed()
    }

    pub fn gen_utf8_data_list_array<O: Offset, O2: Offset>(
        n: usize,
        nulls: Option<usize>,
    ) -> ListArray<O> {
        let mut vals = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                vals.push(None);
            } else {
                vals.push(Some(vec![format!("{idx}")]));
            }
        }

        create_list_utf8_array::<O, O2, _, _>(vals)
    }

    pub fn gen_binary_data_list_array<O: Offset, O2: Offset>(
        n: usize,
        nulls: Option<usize>,
    ) -> ListArray<O> {
        let mut ret = Vec::with_capacity(n);

        for idx in 0..n {
            if nulls.is_some() && idx % nulls.unwrap() == 0 {
                ret.push(None);
            } else {
                ret.push(Some(vec![format!("{idx}")]));
            }
        }

        create_list_binary_array::<O, O2, _, _>(ret)
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

    pub fn create_list_primitive_array<
        O: Offset,
        N: NativeType,
        U: AsRef<[N]>,
        T: AsRef<[Option<U>]>,
    >(
        data: T,
        dt: DataType,
    ) -> ListArray<O> {
        let iter = data.as_ref().iter().map(|x| {
            x.as_ref()
                .map(|x| x.as_ref().iter().map(|x| Some(*x)).collect::<Vec<_>>())
        });

        let inner_dt = if let DataType::List(inner) = &dt {
            inner.data_type().to_owned()
        } else if let DataType::LargeList(inner) = &dt {
            inner.data_type().to_owned()
        } else {
            unreachable!()
        };

        let mut array =
            MutableListArray::new_from(MutablePrimitiveArray::<N>::new().to(inner_dt), dt, 0);
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

    pub fn create_list_utf8_array<
        O: Offset,
        O2: Offset,
        U: AsRef<[String]>,
        T: AsRef<[Option<U>]>,
    >(
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
        let mut array = MutableListArray::<O, MutableUtf8Array<O2>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    pub fn create_list_binary_array<
        O: Offset,
        O2: Offset,
        U: AsRef<[String]>,
        T: AsRef<[Option<U>]>,
    >(
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
        let mut array = MutableListArray::<O, MutableBinaryArray<O2>>::new();
        array.try_extend(iter).unwrap();
        array.into()
    }

    // Parses a markdown table into a vector of arrays:
    //  * Types supported: Int64, Int32, Float64, Boolean, Utf8, List
    //  * List types supported: Int64, Int32, Float64, Boolean, Utf8
    //  * List should be only one level deep
    //
    // # Arguments
    // * `data`   - The markdown table. The first row must be the header. You can use a list of values
    //              separated by commas to create a list array
    // * `fields` - The fields of the table
    //
    // # Example
    //     let data = r#"
    // ```markdown
    // | a | b     | c    | d     | e     |
    // |---|-------|------|-------|-------|
    // | 1 | true  | test | 1,2,3 | a,b,c |
    // | 2 |       |      | 1,2   | b     |
    // | 3 | false | lala |       |       |
    // ```
    //     "#;
    //     let fields = vec![
    //         Field::new("a", DataType::Int64, true),
    //         Field::new("b", DataType::Boolean, true),
    //         Field::new("c", DataType::Utf8, true),
    //         Field::new("d", DataType::List(Box::new(Field::new("1", DataType::Int32, true))), true),
    //         Field::new("e", DataType::List(Box::new(Field::new("1", DataType::Utf8, true))), true),
    //     ];
    //
    //     let parsed = parse_markdown_table(data.to_string(), &fields)?;
    //     println!("{:#?}", parsed);
    //
    //    // Output:
    //    // [
    //    //  Int64[1, 2, 3],
    //    //  BooleanArray[true, None, false],
    //    //  LargeUtf8Array[test, None, lala],
    //    //  ListArray[[1, 2, 3], [1, 2], None],
    //    //  ListArray[[a, b, c], [b], None],
    //    // ]
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
                DataType::List(inner) => match inner.data_type() {
                    DataType::Int64 => {
                        let vals: Vec<Option<Vec<i64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals, field.data_type.clone())
                            .boxed()
                    }
                    DataType::Int32 => {
                        let vals: Vec<Option<Vec<i32>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals, field.data_type.clone())
                            .boxed()
                    }
                    DataType::Float64 => {
                        let vals: Vec<Option<Vec<f64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i32, _, _, _>(vals, field.data_type.clone())
                            .boxed()
                    }
                    DataType::Boolean => {
                        let vals: Vec<Option<Vec<bool>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_bool_array::<i32, _, _>(vals).boxed()
                    }
                    DataType::Utf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i32, i32, _, _>(vals).boxed()
                    }
                    DataType::LargeUtf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i32, i64, _, _>(vals).boxed()
                    }
                    _ => unimplemented!(),
                },
                DataType::LargeList(inner) => match inner.data_type {
                    DataType::Int64 => {
                        let vals: Vec<Option<Vec<i64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals, inner.data_type.clone())
                            .boxed()
                    }
                    DataType::Int32 => {
                        let vals: Vec<Option<Vec<i32>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals, inner.data_type.clone())
                            .boxed()
                    }
                    DataType::Float64 => {
                        let vals: Vec<Option<Vec<f64>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_primitive_array::<i64, _, _, _>(vals, inner.data_type.clone())
                            .boxed()
                    }
                    DataType::Boolean => {
                        let vals: Vec<Option<Vec<bool>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_bool_array::<i64, _, _>(vals).boxed()
                    }
                    DataType::Utf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i64, i32, _, _>(vals).boxed()
                    }
                    DataType::LargeUtf8 => {
                        let vals: Vec<Option<Vec<String>>> =
                            vals.into_iter().map(|v| v.into()).collect::<Vec<_>>();
                        create_list_utf8_array::<i64, i64, _, _>(vals).boxed()
                    }
                    _ => unimplemented!(),
                },
                _ => unimplemented!(),
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    // generates chunk with index columns and data columns
    pub fn gen_chunk_for_parquet(
        fields: &[Field],
        idx_fields: usize,
        primary_idx_type: PrimaryIndexType,
        nulls_periodicity: Option<usize>,
    ) -> Chunk<Box<dyn Array>> {
        let idx_arrs = match idx_fields {
            1 => {
                let arr = match &fields[0].data_type.to_physical_type() {
                    PhysicalType::Primitive(pt) => gen_idx_primitive_array_from_arrow_type(
                        pt,
                        fields[0].data_type.clone(),
                        primary_idx_type,
                    ),
                    _ => unimplemented!("only support primitive type for idx field"),
                };
                vec![arr]
            }
            2 => {
                let arr1 = match &fields[0].data_type.to_physical_type() {
                    PhysicalType::Primitive(pt) => gen_idx_primitive_array_from_arrow_type(
                        pt,
                        fields[0].data_type.clone(),
                        primary_idx_type.clone(),
                    ),
                    _ => unimplemented!("only support primitive type for idx field"),
                };
                let arr2 = match &fields[1].data_type.to_physical_type() {
                    PhysicalType::Primitive(pt) => {
                        if let PrimaryIndexType::Partitioned(max_partition_size) = primary_idx_type
                        {
                            gen_secondary_idx_primitive_array_from_arrow_type(
                                pt,
                                fields[1].data_type.clone(),
                                max_partition_size,
                            )
                        } else {
                            unimplemented!("only support partition for secondary idx field")
                        }
                    }
                    _ => unimplemented!("only support primitive type for idx field"),
                };
                vec![arr1, arr2]
            }
            _ => unimplemented!("only 1 or 2 idx fields are supported"),
        };

        let len = idx_arrs[0].len();

        let data_arrs = fields
            .iter()
            .skip(idx_fields)
            .map(|field| {
                let nulls_periodicity = if !field.is_nullable {
                    None
                } else {
                    nulls_periodicity
                };
                match &field.data_type.to_physical_type() {
                    PhysicalType::Boolean => gen_boolean_data_array(len, nulls_periodicity).boxed(),
                    PhysicalType::Primitive(pt) => gen_primitive_data_array_from_arrow_type(
                        pt,
                        field.data_type.clone(),
                        len,
                        nulls_periodicity,
                    ),
                    PhysicalType::Binary => {
                        gen_binary_data_array::<i32>(len, nulls_periodicity).boxed()
                    }
                    PhysicalType::FixedSizeBinary => {
                        if let DataType::FixedSizeBinary(size) = &field.data_type {
                            gen_fixed_size_binary_data_array(len, nulls_periodicity, *size).boxed()
                        } else {
                            unimplemented!()
                        }
                    }
                    PhysicalType::LargeBinary => {
                        gen_binary_data_array::<i64>(len, nulls_periodicity).boxed()
                    }
                    PhysicalType::Utf8 => {
                        gen_utf8_data_array::<i32>(len, nulls_periodicity).boxed()
                    }
                    PhysicalType::LargeUtf8 => {
                        gen_utf8_data_array::<i64>(len, nulls_periodicity).boxed()
                    }
                    PhysicalType::List => match &field.data_type {
                        DataType::List(inner) => match &inner.data_type.to_physical_type() {
                            PhysicalType::Boolean => {
                                gen_boolean_data_list_array::<i32>(len, nulls_periodicity).boxed()
                            }
                            PhysicalType::Primitive(pt) => {
                                gen_primitive_data_list_array_from_arrow_type::<i32>(
                                    pt,
                                    field.data_type.clone(),
                                    len,
                                    nulls_periodicity,
                                )
                            }
                            PhysicalType::Binary => {
                                gen_binary_data_list_array::<i32, i32>(len, nulls_periodicity)
                                    .boxed()
                            }
                            PhysicalType::LargeBinary => {
                                gen_binary_data_list_array::<i32, i64>(len, nulls_periodicity)
                                    .boxed()
                            }
                            PhysicalType::Utf8 => {
                                gen_utf8_data_list_array::<i32, i32>(len, nulls_periodicity).boxed()
                            }
                            PhysicalType::LargeUtf8 => {
                                gen_utf8_data_list_array::<i32, i64>(len, nulls_periodicity).boxed()
                            }
                            _ => unimplemented!("{:?}", inner.data_type),
                        },
                        _ => unimplemented!(),
                    },
                    PhysicalType::LargeList => match &field.data_type {
                        DataType::LargeList(_inner) => match &field.data_type.to_physical_type() {
                            PhysicalType::Boolean => {
                                gen_boolean_data_list_array::<i64>(len, nulls_periodicity).boxed()
                            }
                            PhysicalType::Primitive(pt) => {
                                gen_primitive_data_list_array_from_arrow_type::<i64>(
                                    pt,
                                    field.data_type.clone(),
                                    len,
                                    nulls_periodicity,
                                )
                            }
                            PhysicalType::Binary => {
                                gen_binary_data_list_array::<i64, i32>(len, nulls_periodicity)
                                    .boxed()
                            }
                            PhysicalType::LargeBinary => {
                                gen_binary_data_list_array::<i64, i64>(len, nulls_periodicity)
                                    .boxed()
                            }
                            PhysicalType::Utf8 => {
                                gen_utf8_data_list_array::<i64, i32>(len, nulls_periodicity).boxed()
                            }
                            PhysicalType::LargeUtf8 => {
                                gen_utf8_data_list_array::<i64, i64>(len, nulls_periodicity).boxed()
                            }
                            _ => unimplemented!("{:?}", field.data_type),
                        },
                        _ => unimplemented!(),
                    },
                    _ => unimplemented!(),
                }
            })
            .collect::<Vec<_>>();

        Chunk::new([idx_arrs, data_arrs].concat().to_vec())
    }

    // split chunk to multiple ones
    pub fn unmerge_chunk(
        chunk: Chunk<Box<dyn Array>>,
        out_count: usize,
        values_per_row_group: usize,
        exclusive_row_groups_periodicity: Option<usize>,
    ) -> Vec<Vec<Chunk<Box<dyn Array>>>> {
        let mut idx = 0;
        let mut cur_row_group = 0;
        let mut res: Vec<Vec<Chunk<Box<dyn Array>>>> = vec![vec![]; out_count];
        while idx < chunk.len() {
            match exclusive_row_groups_periodicity {
                // take chunk exclusively for one stream. To test picking during merge
                Some(n) if cur_row_group % n == 0 => {
                    let end = std::cmp::min(idx + values_per_row_group, chunk.len());
                    // make a slice from original chunk
                    let out = chunk
                        .arrays()
                        .iter()
                        .map(|arr| {
                            let take_idx =
                                PrimitiveArray::from_vec((idx..end).map(|v| v as i64).collect());
                            take(arr.as_ref(), &take_idx).unwrap()
                            // arr.sliced(idx, end - idx) // TODO slice is not working, producing enormous pages amount while writing to parquet
                        })
                        .collect::<Vec<_>>();
                    // round robin stream assignment
                    let stream_id = cur_row_group % out_count;
                    let chunk = Chunk::new(out);
                    res[stream_id].push(chunk);

                    idx += values_per_row_group;
                }
                // split between multiple out chunks. To test actual merge of intersected chunks. Example: slice 1..10, 3 outs. We'll take [1, 4, 7, 10], [2, 5, 8], [3, 6, 9]
                _ => {
                    // try to take values enough to split between all the out chunks
                    let to_take = values_per_row_group * out_count;
                    let end = std::cmp::min(idx + to_take, chunk.len());

                    for (stream_id, chunks) in res.iter_mut().enumerate().take(out_count) {
                        // calculate indexes for each out. Example: slice 1..10, 3 outs. We'll take [1, 4, 7, 10], [2, 5, 8], [3, 6, 9]
                        let take_idx = (0..end - idx)
                            .skip(stream_id)
                            .step_by(out_count)
                            .map(|v| (v + idx) as i64)
                            .collect();
                        let take_idx = PrimitiveArray::from_vec(take_idx);
                        // actual take
                        let out = chunk
                            .arrays()
                            .iter()
                            .map(|arr| take(arr.as_ref(), &take_idx).unwrap())
                            .collect::<Vec<_>>();

                        chunks.push(Chunk::new(out));
                    }

                    idx += to_take;
                }
            }

            cur_row_group += 1;
        }

        res
    }

    pub fn read_parquet_as_one_chunk<R: Read + Seek>(reader: &mut R) -> Chunk<Box<dyn Array>> {
        concat_chunks(read_parquet(reader))
    }

    pub fn read_parquet<R: Read + Seek>(reader: &mut R) -> Vec<Chunk<Box<dyn Array>>> {
        let metadata = read::read_metadata(reader).unwrap();
        let schema = read::infer_schema(&metadata).unwrap();
        let chunks = read::FileReader::new(
            reader,
            metadata.row_groups,
            schema,
            Some(1024 * 1024 * 1024),
            None,
            None,
        );

        chunks.map(|chunk| chunk.unwrap()).collect::<Vec<_>>()
    }

    pub fn concat_chunks(chunks: Vec<Chunk<Box<dyn Array>>>) -> Chunk<Box<dyn Array>> {
        let arrs = (0..chunks[0].arrays().len())
            .map(|arr_id| {
                let to_concat = chunks
                    .iter()
                    .map(|chunk| chunk.arrays()[arr_id].as_ref())
                    .collect::<Vec<_>>();
                concatenate(&to_concat).unwrap()
            })
            .collect::<Vec<_>>();
        Chunk::new(arrs)
    }

    pub fn make_missing_columns(
        chunk: Chunk<Box<dyn Array>>,
        nth: usize,
        shift: usize,
        idx_cols_len: usize,
    ) -> Chunk<Box<dyn Array>> {
        let arrs = chunk
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(col_id, col)| {
                if col_id >= idx_cols_len && (col_id + shift) % nth == 0 {
                    None
                } else {
                    Some(col.to_owned())
                }
            })
            .collect::<Vec<_>>();

        Chunk::new(arrs)
    }

    pub fn make_missing_fields(
        fields: Vec<Field>,
        nth: usize,
        shift: usize,
        idx_cols_len: usize,
    ) -> Vec<Field> {
        fields
            .iter()
            .enumerate()
            .filter_map(|(field_id, field)| {
                if field_id >= idx_cols_len && (field_id + shift) % nth == 0 {
                    None
                } else {
                    Some(field.to_owned())
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn create_parquet_from_chunk<W: Write>(
        chunk: Chunk<Box<dyn Array>>,
        fields: Vec<Field>,
        w: W,
        data_pagesize_limit: Option<usize>,
        values_per_row_group: usize,
    ) -> anyhow::Result<()> {
        let schema = Schema::from(fields);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Snappy,
            version: Version::V2,
            data_pagesize_limit,
        };

        let mut idx = 0;
        let mut chunks = vec![];
        while idx < chunk.len() {
            let end = std::cmp::min(idx + values_per_row_group, chunk.len());
            let arrs = chunk
                .arrays()
                .iter()
                .map(|arr| arr.sliced(idx, end - idx))
                .collect::<Vec<_>>();
            chunks.push(Ok(Chunk::new(arrs)));
            idx += values_per_row_group;
        }

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let row_groups =
            RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;

        let mut writer = FileWriter::try_new(w, schema, options)?;

        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;

        Ok(())
    }

    pub fn create_parquet_from_chunks<W: Write>(
        chunks: Vec<Chunk<Box<dyn Array>>>,
        fields: Vec<Field>,
        w: W,
        data_pagesize_limit: Option<usize>,
    ) -> anyhow::Result<()> {
        let schema = Schema::from(fields);

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V2,
            data_pagesize_limit,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
            .collect();

        let chunks = chunks.into_iter().map(Ok).collect::<Vec<_>>();
        let row_groups =
            RowGroupIterator::try_new(chunks.into_iter(), &schema, options, encodings)?;

        let mut writer = FileWriter::try_new(w, schema, options)?;

        for group in row_groups {
            writer.write(group?)?;
        }
        let _size = writer.end(None)?;

        Ok(())
    }

    // creates parquet file from chunk
    pub fn create_parquet_file_from_chunk(
        chunk: Chunk<Box<dyn Array>>,
        fields: Vec<Field>,
        path: impl AsRef<Path>,
        data_pagesize_limit: Option<usize>,
        values_per_row_group: usize,
    ) -> anyhow::Result<()> {
        // Create a new empty file
        let file = File::create(path)?;
        create_parquet_from_chunk(
            chunk,
            fields,
            file,
            data_pagesize_limit,
            values_per_row_group,
        )
    }
}
