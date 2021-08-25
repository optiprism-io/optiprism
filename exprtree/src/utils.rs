use datafusion::physical_plan::ColumnarValue;
use arrow::array::ArrayRef;

pub fn into_array(cv: ColumnarValue) -> ArrayRef {
    match cv {
        ColumnarValue::Array(array) => array,
        _=>panic!("unexpected"),
    }
}
