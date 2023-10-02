use std::mem;


use std::sync::Arc;

use arrow::ffi::ArrowArray;
use arrow::ffi::ArrowArrayRef;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::IntervalUnit;
use arrow2::datatypes::TimeUnit;
use arrow_array::make_array;
use arrow_array::ArrayRef;


use crate::error::Result;

pub fn arrow2_to_arrow1(
    arr2: Box<dyn arrow2::array::Array>,
    field: Field,
) -> Result<(ArrayRef, arrow_schema::Field)> {
    let arr2_ffi = arrow2::ffi::export_array_to_c(arr2);
    let schema2_ffi = arrow2::ffi::export_field_to_c(&field);

    let arr1_ffi = unsafe { mem::transmute(arr2_ffi) }; // todo get rid of transmute?
    let schema1_ffi = unsafe { mem::transmute(schema2_ffi) };

    let arr = make_array(ArrowArray::new(arr1_ffi, schema1_ffi).try_into()?);
    let f = field2_to_field1(field);
    Ok((arr, f))
}

fn time_unit_2_to_time_unit_1(tu: TimeUnit) -> arrow_schema::TimeUnit {
    match tu {
        TimeUnit::Second => arrow_schema::TimeUnit::Second,
        TimeUnit::Millisecond => arrow_schema::TimeUnit::Millisecond,
        TimeUnit::Microsecond => arrow_schema::TimeUnit::Microsecond,
        TimeUnit::Nanosecond => arrow_schema::TimeUnit::Nanosecond,
    }
}

fn interval_unit2_to_interval_unit1(iu: IntervalUnit) -> arrow_schema::IntervalUnit {
    match iu {
        IntervalUnit::YearMonth => arrow_schema::IntervalUnit::YearMonth,
        IntervalUnit::DayTime => arrow_schema::IntervalUnit::DayTime,
        IntervalUnit::MonthDayNano => arrow_schema::IntervalUnit::MonthDayNano,
    }
}

fn field2_to_field1(field: Field) -> arrow_schema::Field {
    let dt = match field.data_type {
        DataType::Null => arrow_schema::DataType::Null,
        DataType::Boolean => arrow_schema::DataType::Boolean,
        DataType::Int8 => arrow_schema::DataType::Int8,
        DataType::Int16 => arrow_schema::DataType::Int16,
        DataType::Int32 => arrow_schema::DataType::Int32,
        DataType::Int64 => arrow_schema::DataType::Int64,
        DataType::UInt8 => arrow_schema::DataType::UInt8,
        DataType::UInt16 => arrow_schema::DataType::UInt16,
        DataType::UInt32 => arrow_schema::DataType::UInt32,
        DataType::UInt64 => arrow_schema::DataType::UInt64,
        DataType::Float16 => arrow_schema::DataType::Float16,
        DataType::Float32 => arrow_schema::DataType::Float32,
        DataType::Float64 => arrow_schema::DataType::Float64,
        DataType::Timestamp(tu, tz) => {
            arrow_schema::DataType::Timestamp(time_unit_2_to_time_unit_1(tu), tz.map(|s| s.into()))
        }
        DataType::Date32 => arrow_schema::DataType::Date32,
        DataType::Date64 => arrow_schema::DataType::Date64,
        DataType::Time32(tu) => arrow_schema::DataType::Time32(time_unit_2_to_time_unit_1(tu)),
        DataType::Time64(tu) => arrow_schema::DataType::Time64(time_unit_2_to_time_unit_1(tu)),
        DataType::Duration(d) => arrow_schema::DataType::Duration(time_unit_2_to_time_unit_1(d)),
        DataType::Interval(iu) => {
            arrow_schema::DataType::Interval(interval_unit2_to_interval_unit1(iu))
        }
        DataType::Binary => arrow_schema::DataType::Binary,
        DataType::FixedSizeBinary(s) => arrow_schema::DataType::FixedSizeBinary(s as i32),
        DataType::LargeBinary => arrow_schema::DataType::LargeBinary,
        DataType::Utf8 => arrow_schema::DataType::Utf8,
        DataType::LargeUtf8 => arrow_schema::DataType::LargeUtf8,
        DataType::List(l) => arrow_schema::DataType::List(Arc::new(field2_to_field1(*l))),
        DataType::FixedSizeList(l, s) => {
            arrow_schema::DataType::FixedSizeList(Arc::new(field2_to_field1(*l)), s as i32)
        }
        DataType::LargeList(l) => arrow_schema::DataType::LargeList(Arc::new(field2_to_field1(*l))),
        DataType::Struct(s) => {
            arrow_schema::DataType::Struct(s.iter().map(|f| field2_to_field1(f.clone())).collect())
        }
        DataType::Union(_, _, _) => unimplemented!(),
        DataType::Map(_, _) => unimplemented!(),
        DataType::Dictionary(_, _, _) => unimplemented!(),
        DataType::Decimal(p, s) => arrow_schema::DataType::Decimal128(p as u8, s as i8),
        DataType::Decimal256(p, s) => arrow_schema::DataType::Decimal256(p as u8, s as i8),
        DataType::Extension(_, _, _) => unimplemented!(),
    };

    arrow_schema::Field::new(field.name, dt, field.is_nullable)
}

pub mod arrow2_to_arrow1 {
    use std::mem;

    use arrow::ffi::ArrowArray;
    
    use arrow2::datatypes::Field;
    use arrow_array::make_array;

    use crate::error::Result;

    pub fn convert(arr2: Box<dyn arrow2::array::Array>) -> Result<arrow::array::ArrayRef> {
        let field = Field::new("item", arr2.data_type().to_owned(), true);
        let arr2_ffi = arrow2::ffi::export_array_to_c(arr2);
        let schema2_ffi = arrow2::ffi::export_field_to_c(&field);

        let arr1_ffi = unsafe { mem::transmute(arr2_ffi) }; // todo get rid of transmute?
        let schema1_ffi = unsafe { mem::transmute(schema2_ffi) };

        let arr = make_array(ArrowArray::new(arr1_ffi, schema1_ffi).try_into()?);
        Ok(arr)
    }
}

pub mod arrow1_to_arrow2 {
    use std::mem;

    
    use arrow::ffi::ArrowArrayRef;
    use arrow2::array::Array;
    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::ffi::import_array_from_c;
    
    use arrow_array::ArrayRef;
    use arrow_data::ffi::FFI_ArrowArray;
    
    

    use crate::error::Result;

    pub fn convert(arr: ArrayRef) -> Result<Box<dyn Array>> {
        let arr1_ffi = FFI_ArrowArray::new(&arr.to_data());

        let arr2_ffi = unsafe { mem::transmute(arr1_ffi) };

        let data_type = match arr.data_type() {
            arrow::datatypes::DataType::Int64 => DataType::Int64,
            arrow::datatypes::DataType::List(f) => match f.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    DataType::List(Box::new(Field::new("item", DataType::Int64, true)))
                }
                _ => unimplemented!(),
            },
            _ => unimplemented!(),
        };
        let res = unsafe { import_array_from_c(arr2_ffi, data_type) }?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use arrow2::array::Int64Array;
    
    use arrow2::datatypes::DataType as DataType2;
    use arrow2::datatypes::Field as Field2;

    use crate::arrow_conversion::arrow1_to_arrow2;
    use crate::arrow_conversion::arrow2_to_arrow1;
    use crate::test_util::create_list_primitive_array;

    #[test]
    fn test_roundtrip_primitive() {
        let exp = Int64Array::from(vec![Some(1), None, Some(3)]);
        let tmp = arrow2_to_arrow1::convert(exp.clone().boxed()).unwrap();
        let res = arrow1_to_arrow2::convert(tmp).unwrap();

        assert_eq!(res, exp.boxed());
    }

    #[test]
    fn test_roundtrip_list() {
        let dt = DataType2::List(Box::new(Field2::new("item", DataType2::Int64, true)));
        let exp = create_list_primitive_array::<i32, i64, _, _>(vec![Some(vec![1])], dt);
        let tmp = arrow2_to_arrow1::convert(exp.clone().boxed()).unwrap();
        let res = arrow1_to_arrow2::convert(tmp).unwrap();

        assert_eq!(res, exp.boxed());
    }
}
