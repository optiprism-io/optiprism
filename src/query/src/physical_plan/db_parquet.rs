use std::any::Any;
use std::cell::RefCell;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use arrow::array::RecordBatch;
use arrow::datatypes::FieldRef;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::print_batches;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use async_trait::async_trait;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;
use storage::arrow_conversion;
use storage::arrow_conversion::arrow2_to_arrow1;
use storage::db::OptiDBImpl;
use storage::db::ScanStream;

use crate::error::QueryError;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::Result;

#[derive(Debug)]
pub struct DBParquetExec {
    db: Arc<OptiDBImpl>,
    projection: Vec<usize>,
    schema: SchemaRef,
}

impl DBParquetExec {
    pub fn try_new(db: Arc<OptiDBImpl>, projection: Vec<usize>) -> Result<Self> {
        let schema = db.schema1("events")?.project(&projection)?;
        Ok(Self {
            db,
            projection,
            schema: Arc::new(schema),
        })
    }
}

impl DisplayAs for DBParquetExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DbParquetExec")
    }
}

#[async_trait]
impl ExecutionPlan for DBParquetExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            DBParquetExec::try_new(self.db.clone(), self.projection.clone())
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self
            .db
            .scan("events", self.projection.clone())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(Box::pin(ParquetStream {
            stream: Pin::new(Box::new(stream))
                as Pin<
                    Box<dyn Stream<Item = storage::error::Result<Chunk<Box<dyn Array>>>> + Send>,
                >,
            schema: self.schema.clone(),
        }))
    }
}

struct ParquetStream {
    stream: Pin<Box<dyn Stream<Item = storage::error::Result<Chunk<Box<dyn Array>>>> + Send>>,
    schema: SchemaRef,
}

impl Stream for ParquetStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let cols = batch
                    .columns()
                    .into_iter()
                    .map(|c| arrow2_to_arrow1::convert(c.to_owned()))
                    .collect::<storage::error::Result<Vec<_>>>()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                let batch = RecordBatch::try_new(self.schema.clone(), cols)?;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(err))) => {
                Poll::Ready(Some(Err(DataFusionError::Execution(err.to_string()))))
            }
        }
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
