use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::ArrayRef;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::Result;

#[derive(Debug)]
pub struct PartitionExec {
    input: Arc<dyn ExecutionPlan>,
    partition_expr: Vec<Arc<dyn PhysicalExpr>>,
    partition_col_name: String,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_expr: Vec<Arc<dyn PhysicalExpr>>,
        partition_col_name: String,
    ) -> Result<Self> {
        let field = Field::new(partition_col_name.clone(), DataType::UInt64, false);
        let mut schema = (*input.schema()).clone();
        let _a = schema.fields();

        schema.fields = [vec![Arc::new(field)], schema.fields.to_vec()]
            .concat()
            .into();
        Ok(Self {
            input,
            partition_expr,
            partition_col_name,
            schema: Arc::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for PartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionExec")
    }
}

#[async_trait]
impl ExecutionPlan for PartitionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
            PartitionExec::try_new(
                children[0].clone(),
                self.partition_expr.clone(),
                self.partition_col_name.clone(),
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;

        let _baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(PartitionStream {
            hash_buffer: vec![],
            stream,
            schema: self.schema.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            partition_expr: self.partition_expr.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }
}

struct PartitionStream {
    hash_buffer: Vec<u64>,
    stream: SendableRecordBatchStream,
    partition_expr: Vec<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for PartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for PartitionStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();
        let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let poll = match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let arrs = self
                    .partition_expr
                    .iter()
                    .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()).unwrap()))
                    .collect::<DFResult<Vec<_>>>()?;

                self.hash_buffer.clear();
                self.hash_buffer.resize(batch.num_rows(), 0);
                create_hashes(&arrs, &random_state, &mut self.hash_buffer)?;
                let hash_arr = UInt64Array::from(self.hash_buffer.clone());

                let result = RecordBatch::try_new(
                    self.schema.clone(),
                    [
                        vec![Arc::new(hash_arr) as ArrayRef],
                        batch.columns().to_owned(),
                    ]
                    .concat(),
                )?;
                Poll::Ready(Some(Ok(result)))
            }

            other => other,
        };

        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use storage::test_util::parse_markdown_tables;

    use crate::physical_plan::partition::PartitionExec;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|-------|--------|-------------|
| 0            | iphone       | 1     | 1      | e1          |
| 0            | iphone       | 0     | 2      | e2          |
| 0            | iphone       | 0     | 3      | e3          |
| 0            | android      | 1     | 4      | e1          |
| 0            | android      | 1     | 5      | e2          |
| 0            | android      | 0     | 6      | e3          |
| 1            | osx          | 1     | 1      | e1          |
| 1            | osx          | 1     | 2      | e2          |
| 1            | osx          | 0     | 3      | e3          |
| 1            | osx          | 0     | 4      | e1          |
| 1            | osx          | 0     | 5      | e2          |
| 1            | osx          | 0     | 6      | e3          |
| 2            | osx          | 1     | 1      | e1          |
| 2            | osx          | 1     | 2      | e2          |
| 2            | osx          | 0     | 3      | e3          |
| 2            | osx          | 0     | 4      | e1          |
| 2            | osx          | 0     | 5      | e2          |
| 2            | osx          | 0     | 6      | e3          |
| 3            | osx          | 1     | 1      | e1          |
| 3            | osx          | 1     | 2      | e2          |
| 3            | osx          | 0     | 3      | e3          |
| 3            | osx          | 0     | 4      | e1          |
| 3            | osx          | 0     | 5      | e2          |
| 3            | osx          | 0     | 6      | e3          |
| 4            | osx          | 0     | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&[batches], schema.clone(), None)?;

        let seg = PartitionExec::try_new(
            Arc::new(input),
            vec![
                Arc::new(Column::new_with_schema("user_id", &schema)?),
                Arc::new(Column::new_with_schema("device", &schema)?),
            ],
            "hash".to_string(),
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }
}
