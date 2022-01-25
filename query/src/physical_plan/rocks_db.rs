use core::fmt;
use std::any::Any;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use arrow::array::{ArrayData, Int32Builder, ListBuilder, UInt64Builder, UInt8BufferBuilder};
use datafusion::error::Result as DFResult;

use super::{
    common, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use crate::error::{DataFusionError, Result};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::datatypes::DataType::UInt64;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{common, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use rocksdb::DBIterator;

use async_trait::async_trait;
use futures::Stream;

use bincode::{deserialize, serialize};
use datafusion::scalar::ScalarValue;
use metadata::events::Event;



pub struct RocksDBExec<'a, T> {
    writer: T,
    iter: Arc<Mutex<DBIterator<'a>>>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl<T> fmt::Debug for RocksDBExec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "partitions: [...]")?;
        write!(f, "schema: {:?}", self.projected_schema)?;
        write!(f, "projection: {:?}", self.projection)
    }
}

impl<T> RocksDBExec<T> {
    pub fn try_new(
        writer: T,
        iter: Arc<Mutex<DBIterator>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = match &projection {
            Some(columns) => {
                let fields: Result<Vec<Field>> = columns
                    .iter()
                    .map(|i| {
                        if *i < schema.fields().len() {
                            Ok(schema.field(*i).clone())
                        } else {
                            Err(DataFusionError::Internal(
                                "Projection index out of range".to_string(),
                            ))
                        }
                    })
                    .collect();
                Arc::new(Schema::new(fields?))
            }
            None => Arc::clone(&schema),
        };

        Ok(RocksDBExec {
            writer,
            iter,
            schema,
            projected_schema,
            projection,
        })
    }
}

#[async_trait]
impl<T> ExecutionPlan for RocksDBExec<T> where T: Clone {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        &self,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {:?}",
            self
        )))
    }

    async fn execute(&self, _partition: usize) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RocksDBStream::new(
            self.writer.clone(),
            self.iter.clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let partitions: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();
                write!(
                    f,
                    "MemoryExec: partitions={}, partition_sizes={:?}",
                    partitions.len(),
                    partitions
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

/// Iterator over batches
pub(crate) struct RocksDBStream<'a, T> {
    writer: T,
    iter: Arc<DBIterator<'a>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
}

impl<'a, T> RocksDBStream<'a, T> {
    /// Create an iterator for a vector of record batches
    pub fn new(
        writer: T,
        iter: Arc<Mutex<DBIterator>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            writer,
            iter,
            schema,
            projection,
        }
    }
}

impl<'a, T> Stream for RocksDBStream<'a, T> where T: RecordBatchWriter {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        for (k, v) in self.iter {
            match self.writer.write(v) {
                Err(err) => return Poll::Ready(Some(Err(err.into_arrow_external_error()))),
                _ => {}
            }
        }

        Poll::Ready(
            if self.writer.len() > 0 {
                Some(Ok(self.writer.build()))
            } else {
                None
            }
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl<'a, T> RecordBatchStream for RocksDBStream<'a, T> {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}