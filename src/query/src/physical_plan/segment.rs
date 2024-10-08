use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::expressions::segmentation::SegmentExpr;
use crate::Result;

#[derive(Debug)]
pub struct SegmentExec {
    input: Arc<dyn ExecutionPlan>,
    expr: Arc<dyn SegmentExpr>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    partition_col: Column,
    cache: PlanProperties,
}

impl SegmentExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: Arc<dyn SegmentExpr>,
        partition_col: Column,
    ) -> Result<Self> {
        let field = Field::new("partition", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let cache = Self::compute_properties(&input, schema.clone())?;
        Ok(Self {
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            expr,
            partition_col,
            cache,
        })
    }
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            UnknownPartitioning(1),
            input.execution_mode(),
        ))
    }
}

impl DisplayAs for SegmentExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionExec")
    }
}

#[async_trait]
impl ExecutionPlan for SegmentExec {
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentExec::try_new(
                children[0].clone(),
                self.expr.clone(),
                self.partition_col.clone(),
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
        Ok(Box::pin(SegmentStream {
            stream,
            expr: self.expr.clone(),
            schema: self.schema.clone(),
            is_ended: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }
}

struct SegmentStream {
    stream: SendableRecordBatchStream,
    expr: Arc<dyn SegmentExpr>,
    schema: SchemaRef,
    is_ended: bool,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for SegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for SegmentStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();
        loop {
            let res = match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.expr
                        .evaluate(&batch)
                        .map_err(|e| e.into_datafusion_execution_error())?;
                    None
                }
                Poll::Ready(None) => {
                    self.is_ended = true;
                    let res = self
                        .expr
                        .finalize()
                        .map_err(|e| e.into_datafusion_execution_error())?;
                    Some(res)
                }
                other => return other,
            };

            if res.is_none() {
                continue;
            }
            let result = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(res.unwrap())])?;

            return Poll::Ready(Some(Ok(result)));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use storage::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::segment::SegmentExec;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|-------|--------|-------------|
| 0            | iphone       | 1     | 1      | e1          |
| 0            | iphone       | 0     | 2      | e1          |
| 0            | iphone       | 0     | 3      | e3          |
| 0            | android      | 1     | 4      | e1          |
| 0            | android      | 1     | 5      | e2          |
| 0            | android      | 0     | 6      | e3          |
| 1            | osx          | 1     | 1      | e1          |
| 1            | osx          | 1     | 2      | e2          |
| 1            | osx          | 0     | 3      | e3          |
| 1            | osx          | 0     | 4      | e1          |
||||||
| 1            | osx          | 0     | 5      | e2          |
| 1            | osx          | 0     | 6      | e3          |
| 2            | osx          | 1     | 1      | e1          |
| 2            | osx          | 1     | 2      | e2          |
| 2            | osx          | 0     | 3      | e3          |
| 2            | osx          | 0     | 4      | e1          |
| 2            | osx          | 0     | 5      | e1          |
| 2            | osx          | 0     | 6      | e3          |
| 3            | osx          | 1     | 1      | e1          |
||||||
| 3            | osx          | 1     | 2      | e2          |
| 3            | osx          | 0     | 3      | e3          |
| 3            | osx          | 0     | 4      | e1          |
| 3            | osx          | 0     | 5      | e2          |
| 3            | osx          | 0     | 6      | e3          |
||||||
| 4            | osx          | 0     | 6      | e1          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&[batches], schema.clone(), None)?;

        let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let count = Count::<boolean_op::Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &schema).unwrap(),
            Column::new_with_schema("user_id", &schema).unwrap(),
            2,
            TimeRange::None,
        );

        let seg = SegmentExec::try_new(
            Arc::new(input),
            Arc::new(count),
            Column::new_with_schema("user_id", &schema).unwrap(),
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }
}
