use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::new_null_array;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::Result;

#[derive(Debug)]
pub struct MergeExec {
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    names: Option<(String, Vec<String>)>,
    schema: SchemaRef,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl MergeExec {
    pub fn try_new(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        names: Option<(String, Vec<String>)>,
    ) -> Result<Self> {
        let schemas: Vec<Schema> = inputs.iter().map(|i| i.schema().deref().clone()).collect();
        let schema = Schema::try_merge(schemas)?;
        let schema = if let Some((col, _names)) = &names {
            let fields = [
                vec![Field::new(col, DataType::Utf8, false)],
                schema.fields.iter().map(|f| f.deref().to_owned()).collect(),
            ]
            .concat();

            Schema::new(fields)
        } else {
            schema
        };

        let cache = Self::compute_properties(&inputs[0])?;
        Ok(Self {
            inputs,
            names,
            schema: Arc::new(schema),
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> Result<PlanProperties> {
        let eq_properties = input.equivalence_properties().clone();

        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for MergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.inputs.clone()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            MergeExec::try_new(children, self.names.clone())
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut streams: Vec<SendableRecordBatchStream> = vec![];
        for input in self.inputs.iter() {
            let stream = input.execute(partition, context.clone())?;
            streams.push(stream)
        }

        let _baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(MergeStream {
            streams,
            names: self.names.clone().map(|(_, names)| names),
            stream_idx: 0,
            schema: self.schema.clone(),
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

struct MergeStream {
    streams: Vec<SendableRecordBatchStream>,
    names: Option<Vec<String>>,
    stream_idx: usize,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl MergeStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DFResult<RecordBatch>>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();

        loop {
            let stream_idx = self.stream_idx;
            match self.streams[stream_idx].poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let cols = self
                        .schema
                        .fields()
                        .iter()
                        .enumerate()
                        .map(
                            |(idx, field)| match batch.schema().index_of(field.name().as_str()) {
                                Ok(col_idx) => Ok(batch.column(col_idx).clone()),
                                Err(_) => {
                                    if idx == 0
                                        && let Some(names) = &self.names
                                    {
                                        let sv = ScalarValue::from(names[self.stream_idx].clone());
                                        sv.to_array_of_size(batch.column(0).len())
                                    } else {
                                        Ok(new_null_array(field.data_type(), batch.column(0).len()))
                                    }
                                }
                            },
                        )
                        .collect::<DFResult<Vec<ArrayRef>>>()?;

                    let result = RecordBatch::try_new(self.schema.clone(), cols)?;

                    return Poll::Ready(Some(Ok(result)));
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
    }
}

impl RecordBatchStream for MergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for MergeStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Int32Array;
    use arrow::array::Int8Array;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    pub use datafusion_common::Result;

    use crate::physical_plan::merge::MergeExec;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input1 = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    (
                        "name",
                        Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()]))
                            as ArrayRef,
                    ),
                    ("a", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                ])?,
                RecordBatch::try_from_iter(vec![
                    (
                        "name",
                        Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()]))
                            as ArrayRef,
                    ),
                    ("a", Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef),
                ])?,
            ];

            let schema = batches[0].schema();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let input2 = {
            let batches = vec![RecordBatch::try_from_iter_with_nullable(vec![
                (
                    "name",
                    Arc::new(StringArray::from(vec!["b".to_string(), "b".to_string()])) as ArrayRef,
                    true,
                ),
                (
                    "a",
                    Arc::new(Int32Array::from(vec![5, 6])) as ArrayRef,
                    true,
                ),
                ("b", Arc::new(Int8Array::from(vec![1, 2])) as ArrayRef, true),
            ])?];

            let schema = batches[0].schema();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let input3 = {
            let batches = vec![
                RecordBatch::try_from_iter_with_nullable(vec![
                    (
                        "name",
                        Arc::new(StringArray::from(vec!["c".to_string(), "c".to_string()]))
                            as ArrayRef,
                        true,
                    ),
                    (
                        "a",
                        Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
                        true,
                    ),
                    (
                        "c",
                        Arc::new(BooleanArray::from(vec![true, true])) as ArrayRef,
                        true,
                    ),
                ])?,
                RecordBatch::try_from_iter_with_nullable(vec![
                    (
                        "name",
                        Arc::new(StringArray::from(vec!["c".to_string(), "c".to_string()]))
                            as ArrayRef,
                        true,
                    ),
                    (
                        "a",
                        Arc::new(Int32Array::from(vec![9, 10])) as ArrayRef,
                        true,
                    ),
                    (
                        "c",
                        Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
                        true,
                    ),
                ])?,
            ];

            let schema = batches[0].schema();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let mux = MergeExec::try_new(
            vec![input1, input2, input3],
            Some(("test".to_string(), vec![
                "f1".to_string(),
                "f2".to_string(),
                "f3".to_string(),
            ])),
        )
        .unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = mux.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print!("{}", arrow::util::pretty::pretty_format_batches(&result)?);

        Ok(())
    }
}
