use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use async_trait::async_trait;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionMode::Bounded;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::PlanProperties;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;
use storage::arrow_conversion::arrow2_to_arrow1;
use storage::db::OptiDBImpl;

use crate::error::QueryError;
use crate::Result;

#[derive(Debug)]
pub struct DBParquetExec {
    db: Arc<OptiDBImpl>,
    table: String,
    projection: Vec<usize>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl DBParquetExec {
    pub fn try_new(db: Arc<OptiDBImpl>, table: String, projection: Vec<usize>) -> Result<Self> {
        let schema = db.schema1(table.as_str())?.project(&projection)?;
        let cache = Self::compute_properties(Arc::new(schema.clone()));
        Ok(Self {
            db,
            table,
            projection,
            schema: Arc::new(schema),
            cache,
        })
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            UnknownPartitioning(1),
            Bounded,
        )
    }
}

impl DisplayAs for DBParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            DBParquetExec::try_new(self.db.clone(), self.table.clone(), self.projection.clone())
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
            .scan(&self.table, self.projection.clone())
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
    #[allow(clippy::type_complexity)]
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
                    .iter()
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
