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
use arrow::array::{Array, ArrayRef, Float64Array, StringArray, StringBuilder, UInt8Array};
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

#[derive(Debug)]
pub struct DictionaryDecodeExec {
    input: Arc<dyn ExecutionPlan>,
    decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)>,
    schema: SchemaRef,
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
        }))
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
}

impl RecordBatchStream for DictionaryDecodeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

macro_rules! decode_array {
    ($array_ref:expr,$array_type:ident, $dict:expr)=>{{
        let mut result = StringBuilder::new($array_ref.len());
        let src_arr = $array_ref.as_any().downcast_ref::<$array_type>().unwrap();

        for v in src_arr.iter() {
            match v {
                None=>result.append_null(),
                Some(key)=> {
                 let value = $dict.get_value(key as u64).await.map_err(|err|ArrowError::ExternalError(Box::new(err)))?;
                    result.append_value(value);
                }
            }
        }

        Arc::new(result.finish()) as ArrayRef
    }}
}
#[async_trait]
impl Stream for DictionaryDecodeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let columns = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(idx, array_ref)| match self.decode_cols.iter().find(|(col, _)| idx == col.index()) {
                        None => array_ref.clone(),
                        Some((_, dict)) => {
                            match array_ref.data_type() {
                                DataType::UInt8 => decode_array!(array_ref,UInt8,dict),
                                DataType::UInt16 => decode_array!(array_ref,UInt16,dict),
                                DataType::UInt32 => decode_array!(array_ref,UInt32,dict),
                                DataType::UInt64 => decode_array!(array_ref,UInt64,dict),
                                _ => unimplemented!(),
                            }
                        }
                    }).collect();
                Poll::Ready(Some(RecordBatch::try_new(self.schema.clone(), columns)))
            }
            other => other,
        }
    }
}