use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::Column;
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
use futures::Stream;
use futures::StreamExt;
use metadata::dictionaries::SingleDictionaryProvider;

use crate::error::Result;
#[derive(Debug)]
pub struct DictionaryDecodeExec {
    input: Arc<dyn ExecutionPlan>,
    decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    schema: SchemaRef,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl DictionaryDecodeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    ) -> Result<Self> {
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                match decode_cols
                    .iter()
                    .find(|(col, _)| col.name() == field.name().as_str())
                {
                    None => field.clone(),
                    Some(_) => FieldRef::new(Field::new(
                        field.name(),
                        DataType::Utf8,
                        field.is_nullable(),
                    )),
                }
            })
            .collect::<Vec<_>>();

        let schema = Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            input,
            decode_cols,
            schema,
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

impl DisplayAs for DictionaryDecodeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DictionaryDecodeExec")
    }
}

#[async_trait]
impl ExecutionPlan for DictionaryDecodeExec {
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
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DictionaryDecodeExec::try_new(
            children[0].clone(),
            self.decode_cols.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;

        Ok(Box::pin(DictionaryDecodeStream {
            stream,
            schema: self.schema.clone(),
            decode_cols: self.decode_cols.clone(),
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

struct DictionaryDecodeStream {
    stream: SendableRecordBatchStream,
    decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

macro_rules! decode_array {
    ($array_ref:expr,$array_type:ident, $dict:expr) => {{
        let mut result = StringBuilder::with_capacity($array_ref.len(), $array_ref.len());
        let src_arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();
        for v in src_arr.iter() {
            match v {
                None => result.append_null(),
                Some(key) => {
                    let value = $dict
                        .get_value(key as u64)
                        .map_err(|err| ArrowError::ExternalError(Box::new(err)))
                        .unwrap();
                    result.append_value(value);
                }
            }
        }

        Arc::new(result.finish()) as ArrayRef
    }};
}

impl DictionaryDecodeStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DFResult<RecordBatch>>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();

        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let columns = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(idx, array_ref)| {
                        match self.decode_cols.iter().find(|(col, _)| idx == col.index()) {
                            None => array_ref.to_owned(),
                            Some((_, dict)) => match array_ref.data_type() {
                                DataType::Int8 => decode_array!(array_ref, Int8Array, dict),
                                DataType::Int16 => decode_array!(array_ref, Int16Array, dict),
                                DataType::Int32 => decode_array!(array_ref, Int32Array, dict),
                                DataType::Int64 => decode_array!(array_ref, Int64Array, dict),
                                _ => unimplemented!("{:?}", array_ref.data_type()),
                            },
                        }
                    })
                    .collect();
                Poll::Ready(Some(Ok(RecordBatch::try_new(
                    self.schema.clone(),
                    columns,
                )?)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for DictionaryDecodeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for DictionaryDecodeStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);

        self.baseline_metrics.record_poll(poll)
    }
}
