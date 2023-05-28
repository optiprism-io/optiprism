use arrow2::array;
use arrow2::array::{Array, BooleanArray, PrimitiveArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array, BinaryArray, Utf8Array, Int128Array};
use arrow2::datatypes::{DataType, IntervalUnit, TimeUnit};
use arrow2::datatypes::SchemaRef;

macro_rules! static_array_enum_variant {
    ($variant:ident) => {
        $variant($variantArray)
    }
}

macro_rules! static_array_enum {
    ($($ident:ident)+) => {
        #[derive(Debug,Clone)]
        pub enum StaticArray {
            $($ident(concat_idents!($ident,Array)),)+
        }
    }
}

type ArrayRef = Box<dyn Array>;
type SmallBinaryArray = BinaryArray<i32>;
type LargeBinaryArray = inaryArray<i64>;
type SmallUtf8Array = Utf8Array<i32>;
type LargeUtf8Array = Utf8Array<i64>;

static_array_enum! {
    Int8
    Int16
    Int32
    Int64
    Int128
    UInt8
    UInt16
    UInt32
    UInt64
    Float32
    Float64
    Boolean
    FixedSizeBinary
    SmallBinary
    LargeBinary
    SmallUtf8
    LargeUtf8
}


macro_rules! static_array_variant_from_array {
    ($typ:ident) => {
        DataType::$typ=>StaticArray::$typ(arr.as_any().downcast_ref::<$typArray>().unwrap().clone()),
    }
}

macro_rules! impl_into_static_array_simple {
    ($($ident:ident)+) => {
     $(DataType::$ident=>StaticArray::$ident(arr.as_any().downcast_ref::<concat_idents!($ident,Array)>().unwrap().clone())),+
    }
}

macro_rules! impl_into_static_array {
    ($($ident:ident)+) => {
    impl From<ArrayRef> for StaticArray {
        fn from(arr: ArrayRef) -> Self {
            match arr.data_type() {
                 $(DataType::$ident=>StaticArray::$ident(arr.as_any().downcast_ref::<concat_idents!($ident,Array)>().unwrap().clone())),+
            }
        }
        }
    }
}

macro_rules! impl_into_static_array {
    ($($ident:ident)+) => {
        impl StaticArray {
    fn eq_values(&self, left_row_id: usize, right_row_id: usize) -> bool {
        match self {
             $(StaticArray::$ident(a)=>a.value(left_row_id) == a.value(right_row_id)),+
        }
        }
    }
    }
}

impl_into_static_array!( Int8 Int16 Int32 Int64 Int128 UInt8 UInt16 UInt32 UInt64 Float16 Float32 Float64 Boolean
    TimestampNanosecond TimestampMicrosecond TimestampMillisecond  Time32Second Time32Millisecond  Int64 Int64 Int64 Int64 Int64 Int64 Int64 Int64 Date32 Date64 Int64
    TimestampSecond FixedSizeBinary Binary LargeBinary String LargeString Decimal128 Decimal256);

impl From<ArrayRef> for StaticArray {
    fn from(arr: ArrayRef) -> Self {
        match arr.data_type() {
            DataType::Boolean => StaticArray::Boolean(arr.as_any().downcast_ref::<BooleanArray>().unwrap().clone()),
            DataType::Int8 => StaticArray::Int8(arr.as_any().downcast_ref::<Int8Array>().unwrap().clone()),
            DataType::Int16 => StaticArray::Int16(arr.as_any().downcast_ref::<Int16Array>().unwrap().clone()),
            DataType::Int32 => StaticArray::Int32(arr.as_any().downcast_ref::<Int32Array>().unwrap().clone()),
            DataType::Int64 => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
            DataType::UInt8 => StaticArray::UInt8(arr.as_any().downcast_ref::<UInt8Array>().unwrap().clone()),
            DataType::UInt16 => StaticArray::UInt16(arr.as_any().downcast_ref::<UInt16Array>().unwrap().clone()),
            DataType::UInt32 => StaticArray::UInt32(arr.as_any().downcast_ref::<UInt32Array>().unwrap().clone()),
            DataType::UInt64 => StaticArray::UInt64(arr.as_any().downcast_ref::<UInt64Array>().unwrap().clone()),
            DataType::Float32 => StaticArray::Float32(arr.as_any().downcast_ref::<Float32Array>().unwrap().clone()),
            DataType::Float64 => StaticArray::Float64(arr.as_any().downcast_ref::<Float64Array>().unwrap().clone()),
            DataType::Timestamp(tu, _) => match tu {
                TimeUnit::Second => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Millisecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Microsecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Nanosecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
            }
            DataType::Date32 => StaticArray::Int32(arr.as_any().downcast_ref::<Int32Array>().unwrap().clone()),
            DataType::Date64 => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
            DataType::Time32(tu) => match tu {
                TimeUnit::Second => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Millisecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                _ => unimplemented!()
            }
            DataType::Time64(tu) => match tu {
                TimeUnit::Microsecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Nanosecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                _ => unimplemented!()
            }
            DataType::Duration(tu) => match tu {
                TimeUnit::Second => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Millisecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Microsecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                TimeUnit::Nanosecond => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
            }
            DataType::Interval(i) => match i {
                IntervalUnit::YearMonth => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                IntervalUnit::DayTime => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
                IntervalUnit::MonthDayNano => StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone()),
            }
            DataType::Binary => StaticArray::SmallBinary(arr.as_any().downcast_ref::<SmallBinaryArray>().unwrap().clone()),
            DataType::FixedSizeBinary(_) => StaticArray::FixedSizeBinary(arr.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap().clone()),
            DataType::LargeBinary => StaticArray::LargeBinary(arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap().clone()),
            DataType::Utf8 => StaticArray::SmallUtf8(arr.as_any().downcast_ref::<SmallUtf8Array>().unwrap().clone()),
            DataType::LargeUtf8 => StaticArray::LargeUtf8(arr.as_any().downcast_ref::<LargeUtf8Array>().unwrap().clone()),
            DataType::Decimal(_, _) => StaticArray::Int128(arr.as_any().downcast_ref::<Int128Array>().unwrap().clone()),
            _ => unimplemented!(),
        }
    }
}

pub fn concatenate(l:&StaticArray,r:&StaticArray) -> Result<StaticArray> {

}