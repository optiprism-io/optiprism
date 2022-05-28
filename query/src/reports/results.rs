use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
use crate::Result;

pub struct Series {
    pub dimension_headers: Vec<String>,
    pub metric_headers: Vec<String>,
    pub dimensions: Vec<Vec<ScalarValue>>,
    pub series: Vec<Vec<ScalarValue>>,
}

impl Series {
    // todo remove, use record batches and convert right into api in platform
    pub fn try_from_batch_record(batch: &RecordBatch, dimension_headers: Vec<String>, metric_headers: Vec<String>) -> Result<Self> {
        let mut dimensions: Vec<Vec<ScalarValue>> = vec![vec![]; dimension_headers.len()];
        for (col_idx, header) in dimension_headers.iter().enumerate() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                dimensions[col_idx].push(ScalarValue::try_from_array(arr, idx)?);
            }
        }

        let mut series: Vec<Vec<ScalarValue>> = vec![vec![]; metric_headers.len()];
        for (col_idx, header) in metric_headers.iter().enumerate() {
            let arr = batch.column(batch.schema().index_of(header)?);
            for idx in 0..arr.len() {
                series[col_idx].push(ScalarValue::try_from_array(arr, idx)?);
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