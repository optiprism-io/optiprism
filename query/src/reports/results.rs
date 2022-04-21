use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use crate::Result;

pub struct Series {
    pub dimension_headers: Vec<String>,
    pub metric_headers: Vec<String>,
    pub dimensions: Vec<ScalarValue>,
    pub series: Vec<ScalarValue>,
}

impl Series {
    pub fn try_from_batch_record(batch: &RecordBatch, dimension_headers: Vec<String>, metric_headers: Vec<String>) -> Result<Self> {
        let mut dimensions: Vec<ScalarValue> = vec![];
        for header in dimension_headers.iter() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                dimensions.push(ScalarValue::try_from_array(arr, idx)?);
            }
        }

        let mut series: Vec<ScalarValue> = vec![];
        for header in metric_headers.iter() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                series.push(ScalarValue::try_from_array(arr, idx)?);
            }
        }

        Ok(Series {
            dimension_headers,
            metric_headers,
            dimensions,
            series,
        })
    }
}