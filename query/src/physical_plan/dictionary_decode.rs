use std::any::Any;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use std::fmt;
use ahash::RandomState;
use std::pin::Pin;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
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
    dicts: Vrc<Arc<SingleDictionaryProvider>>,
    decode_cols: Vec<Column>,
    schema: SchemaRef,
}

impl DictionaryDecodeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, decode_cols: Vec<Column>, dicts: Vec<Arc<SingleDictionaryProvider>>) -> Self {
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|field| {
                match decode_cols.iter().find(|col| col.name() == field.name().as_str()) {
                    None => field.clone(),
                    Some(_) => Field::new(field.name(), DataType::Utf8, field.is_nullable()),
                }
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));
        Self {
            input,
            dicts,
            decode_cols,
            schema,
        }
    }
}