use std::sync::Arc;
use arrow::array::{Int32Array, StringArray, ArrayRef};
use arrow::datatypes::{Schema, Field, DataType};
use arrow::record_batch::RecordBatch;
use std::ops::Deref;
use arrow::datatypes::DataType::Utf8;

fn main() -> arrow::error::Result<()> {
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let str_array = StringArray::from(vec!["a", "b"]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("str", DataType::Utf8, false)
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array), Arc::new(str_array)],
    )?;

    let a: ArrayRef
    let a = batch.columns()[0].as_any().downcast_ref::<Int32Array>().unwrap();
    unsafe { let b = a.value_unchecked(1); }
    let b = batch.columns()[1].as_any().downcast_ref::<StringArray>().unwrap();
    let s = b.value();
    Ok(())
}