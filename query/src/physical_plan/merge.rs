use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use futures::executor::block_on;
use futures::{Stream, StreamExt};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::cross_join::stats_cartesian_product;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion_common::{DataFusionError, ScalarValue};
use crate::Result;
use axum::{async_trait};
use arrow::error::Result as ArrowResult;
use datafusion_common::Result as DFResult;
use crate::Error;

pub struct MergeExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    schema: SchemaRef,
}

impl MergeExec {
    pub fn try_new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self> {
        let schemas: Vec<Schema> = inputs.iter().map(|i| i.schema().deref().clone()).collect();
        let schema = Schema::try_merge(schemas)?;

        Ok(Self {
            inputs,
            schema: Arc::new(schema),
        })
    }
}

impl Debug for MergeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MergeExec")
    }
}

#[async_trait]
impl ExecutionPlan for MergeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.inputs[0].output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { self.inputs.clone() }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeExec::try_new(children).map_err(Error::into_datafusion_execution_error)?))
    }

    async fn execute(&self, partition: usize, runtime: Arc<RuntimeEnv>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let mut streams: Vec<SendableRecordBatchStream> = vec![];
        for input in self.inputs.iter() {
            let stream = input.execute(partition, runtime.clone()).await?;
            streams.push(stream)
        }

        Ok(Box::pin(MergeStream {
            streams,
            stream_idx: 0,
            schema: self.schema.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct MergeStream {
    streams: Vec<SendableRecordBatchStream>,
    stream_idx: usize,
    schema: SchemaRef,
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for MergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let stream_idx = self.stream_idx;
            match self.streams[stream_idx].poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let cols = self.schema.fields().iter().map(|field| {
                        match batch.schema().index_of(field.name().as_str()) {
                            Ok(col_idx) => Ok(batch.columns()[col_idx].clone()),
                            Err(_) => {
                                let v = ScalarValue::try_from(field.data_type())?;
                                Ok(v.to_array_of_size(batch.column(0).len()))
                            }
                        }
                    }).collect::<DFResult<Vec<ArrayRef>>>()?;

                    return Poll::Ready(Some(Ok(RecordBatch::try_new(self.schema.clone(), cols)?)));
                }
                Poll::Ready(None) => {
                    if self.stream_idx >= self.streams.len() - 1 {
                        return Poll::Ready(None);
                    }
                    self.stream_idx += 1;
                }
                other => return other,
            }
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int8Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::{Stream, StreamExt};
    pub use datafusion::error::Result;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::memory::MemoryExec;
    use crate::physical_plan::merge::MergeExec;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input1 = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    ("name", Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()])) as ArrayRef),
                    ("a", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                ])?,
                RecordBatch::try_from_iter(vec![
                    ("name", Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()])) as ArrayRef),
                    ("a", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
                ])?,
            ];

            let schema = batches[0].schema().clone();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let input2 = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    ("name", Arc::new(StringArray::from(vec!["b".to_string(), "b".to_string()])) as ArrayRef),
                    ("a", Arc::new(Int32Array::from(vec![5, 6])) as ArrayRef),
                    ("b", Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef),
                ])?,
            ];

            let schema = batches[0].schema().clone();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let input3 = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    ("name", Arc::new(StringArray::from(vec!["c".to_string(), "c".to_string()])) as ArrayRef),
                    ("a", Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef),
                    ("c", Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef),
                ])?,
                RecordBatch::try_from_iter(vec![
                    ("name", Arc::new(StringArray::from(vec!["c".to_string(), "c".to_string()])) as ArrayRef),
                    ("a", Arc::new(Int32Array::from(vec![9, 10])) as ArrayRef),
                    ("c", Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef),
                ])?,
            ];

            let schema = batches[0].schema().clone();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let mux = MergeExec::try_new(vec![input1, input2, input3]).unwrap();
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::new())?);
        let stream = mux.execute(0, runtime).await?;
        let result = collect(stream).await?;

        print!("{}", arrow::util::pretty::pretty_format_batches(&result)?);

        Ok(())
    }
}