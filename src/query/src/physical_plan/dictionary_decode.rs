use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::StringBuilder;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use futures::executor::block_on;
use futures::Stream;
use futures::StreamExt;
use metadata::dictionaries::provider_impl::SingleDictionaryProvider;

pub struct DictionaryDecodeExec {
    input: Arc<dyn ExecutionPlan>,
    decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DictionaryDecodeExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    ) -> Self {
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
                    Some(_) => Field::new(field.name(), DataType::Utf8, field.is_nullable()),
                }
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        Self {
            input,
            decode_cols,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
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

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DictionaryDecodeExec::new(
            children[0].clone(),
            self.decode_cols.clone(),
        )))
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

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DictionaryDecodeExec")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl Debug for DictionaryDecodeExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DictionaryDecodeExec")
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
                    let value = block_on($dict.get_value(key as u64))
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
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
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
                                DataType::UInt8 => decode_array!(array_ref, UInt8Array, dict),
                                DataType::UInt16 => decode_array!(array_ref, UInt16Array, dict),
                                DataType::UInt32 => decode_array!(array_ref, UInt32Array, dict),
                                DataType::UInt64 => decode_array!(array_ref, UInt64Array, dict),
                                _ => unimplemented!(),
                            },
                        }
                    })
                    .collect();
                Poll::Ready(Some(RecordBatch::try_new(self.schema.clone(), columns)))
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
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);

        self.baseline_metrics.record_poll(poll)
    }
}
