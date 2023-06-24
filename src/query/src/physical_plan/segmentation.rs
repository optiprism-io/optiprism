use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::Result as DFResult;
use datafusion_common::Statistics;
use datafusion_expr::ColumnarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
pub struct SegmentationExec {
    predicate: Arc<dyn SegmentationExpr>,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    out_batch_size: usize,
}

impl SegmentationExec {
    pub fn try_new(
        predicate: Arc<dyn SegmentationExpr>,
        partition_key: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let res = Self {
            predicate,
            input,
            metrics: Default::default(),
            partition_key,
            out_batch_size,
        };

        Ok(res)
    }
}

#[async_trait]
impl ExecutionPlan for SegmentationExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentationExec::try_new(
                self.predicate.clone(),
                self.partition_key.clone(),
                children[0].clone(),
                self.out_batch_size,
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(SegmentationStream {
            predicate: self.predicate.clone(),
            partition_key: self.partition_key.clone(),
            input: self.input.execute(partition, context.clone())?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            is_ended: false,
            out_batch_size: self.out_batch_size,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentationExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
pub struct SegmentationStream {
    predicate: Arc<dyn SegmentationExpr>,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    is_ended: bool,
    out_batch_size: usize,
}

impl Stream for SegmentationStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];

        loop {
            let res = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    hash_buf.resize(batch.num_rows(), 0);
                    let pk_arrs = self
                        .partition_key
                        .iter()
                        .map(|v| v.evaluate(&batch))
                        .collect::<std::result::Result<Vec<ColumnarValue>, _>>()?
                        .iter()
                        .map(|v| v.clone().into_array(batch.num_rows()).clone())
                        .collect::<Vec<ArrayRef>>();
                    create_hashes(&pk_arrs, &mut random_state, &mut hash_buf).unwrap();

                    let res = self
                        .predicate
                        .evaluate(&batch, &hash_buf)
                        .map_err(|e| e.into_datafusion_execution_error())?;
                    res
                }
                Poll::Ready(None) => {
                    let res = self
                        .predicate
                        .finalize()
                        .map_err(|e| e.into_datafusion_execution_error())?;

                    Some(res)
                }
                other => return other,
            };

            // match res {
            // None => {}
            // Some(res) => {
            // res.as_any().downcast_ref::<BooleanArray>().unwrap();
            // }
            // }
        }
    }
}
impl RecordBatchStream for SegmentationStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Float32Array;
    use arrow::array::Int16Array;
    use arrow::array::Int64Array;
    use arrow::array::Int64Builder;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use segmentation::SegmentationExpr;

    use crate::physical_plan::expressions::segmentation;
    use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
    use crate::physical_plan::expressions::segmentation::AggregateFunction;
    use crate::physical_plan::segmentation::SegmentationExec;
    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int16, false),
        ]));
        let batches = {
            let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3]));
            let batch1 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            let col1: ArrayRef = Arc::new(Int64Array::from(vec![4, 1, 1, 2, 2, 2, 3, 3, 3]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3]));
            let batch1 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![4, 5, 6, 1]));
            let batch2 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            vec![batch1, batch2]
        };

        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let mut agg = Aggregate::<i16, i64, _>::try_new(
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Int64Builder::with_capacity(10_000),
        )
        .unwrap();

        let segmentation = SegmentationExec::try_new(
            Arc::new(agg),
            vec![Arc::new(Column::new_with_schema("col1", &schema)?)],
            Arc::new(input),
            1,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = segmentation.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        println!("{}", pretty_format_batches(&result)?);
        Ok(())
    }
}
