use arrow::array::ArrayRef;
use datafusion::physical_plan::ColumnarValue;

pub fn into_array(cv: ColumnarValue) -> ArrayRef {
    match cv {
        ColumnarValue::Array(array) => array,
        _ => panic!("unexpected"),
    }
}
