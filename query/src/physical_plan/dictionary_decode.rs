use std::any::Any;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::fmt;
use std::fmt::{Debug, Formatter};
use ahash::RandomState;
use std::pin::Pin;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::array::{Array, ArrayRef, Float64Array, StringArray, StringBuilder, UInt8Array, UInt16Array, UInt32Array, UInt64Array};
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use fnv::FnvHashMap;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion_common::ScalarValue;
use crate::{Result, Error};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result as DFResult;
use axum::{async_trait};
use metadata::dictionaries;
use metadata::dictionaries::provider::SingleDictionaryProvider;
use futures::executor::block_on;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};

pub struct DictionaryDecodeExec {
    input: Arc<dyn ExecutionPlan>,
    decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl DictionaryDecodeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>) -> Self {
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                match decode_cols.iter().find(|(col, _)| col.name() == field.name().as_str()) {
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

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DictionaryDecodeExec::new(
            children[0].clone(),
            self.decode_cols.clone(),
        )))
    }

    async fn execute(&self, partition: usize, runtime: Arc<RuntimeEnv>) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, runtime.clone()).await?;

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
    ($array_ref:expr,$array_type:ident, $dict:expr)=>{{
        let mut result = StringBuilder::new($array_ref.len());
        let src_arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();

        for v in src_arr.iter() {
            match v {
                None=>result.append_null().unwrap(),
                Some(key)=> {
                 let value = block_on($dict.get_value(key as u64)).map_err(|err|ArrowError::ExternalError(Box::new(err))).unwrap();
                    result.append_value(value).unwrap();
                }
            }
        }

        Arc::new(result.finish()) as ArrayRef
    }}
}

impl DictionaryDecodeStream {
    fn poll_next_inner(self: &mut Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<ArrowResult<RecordBatch>>> {
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
                            Some((_, dict)) => {
                                match array_ref.data_type() {
                                    DataType::UInt8 => decode_array!(array_ref,UInt8Array,dict),
                                    DataType::UInt16 => decode_array!(array_ref,UInt16Array,dict),
                                    DataType::UInt32 => decode_array!(array_ref,UInt32Array,dict),
                                    DataType::UInt64 => decode_array!(array_ref,UInt64Array,dict),
                                    _ => unimplemented!(),
                                }
                            }
                        }
                    }).collect();
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