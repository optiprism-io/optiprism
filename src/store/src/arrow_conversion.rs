use std::mem;

use arrow_array::make_array;

use crate::error::Result;

pub fn arrow2_to_arrow1(
    arr2: Box<dyn arrow2::array::Array>,
    field: arrow2::datatypes::Field,
) -> Result<arrow_array::ArrayRef> {
    //let arr2_ffi = arrow2::ffi::export_array_to_c(arr2);
    //let schema2_ffi = arrow2::ffi::export_field_to_c(&field);

    //let arr1_ffi = unsafe { mem::transmute(arr2_ffi) }; // todo get rid of transmute?
    //let schema1_ffi = unsafe { mem::transmute(schema2_ffi) };

    //Ok(make_array(
    //    arrow::ffi::FFI_ArrowArray::new(arr2.data_type()).try_into().unwrap()//, schema1_ffi).try_into()?,
    //))
    todo!("understand requirement")
}
