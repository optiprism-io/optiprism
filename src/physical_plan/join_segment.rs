use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, RecordBatchStream, ColumnarValue, PhysicalExpr};
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use std::any::Any;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::merge::MergeExec;
use futures::{Stream, StreamExt, TryStreamExt};
use std::task::{Context, Poll};
use std::pin::Pin;
use arrow::record_batch::RecordBatch;
use arrow::error::{Result as ArrowResult, ArrowError};
use datafusion::physical_plan::expressions::col;
use arrow::array::{ArrayRef, Int8Array, Array, DynComparator, BooleanArray, StringArray, MutableArrayData};
use crate::physical_plan::utils::into_array;
use datafusion::logical_plan::Operator;
use crate::expression_tree::multibatch::expr::Expr;
use arrow::compute::kernels;
use std::collections::HashMap;
use arrow::ipc::SchemaBuilder;
use std::borrow::BorrowMut;
use datafusion::scalar::ScalarValue;
use std::alloc::Global;
use arrow::buffer::MutableBuffer;

pub type JoinOn = (String, String);

#[derive(Clone)]
struct Segment {
    name: String,
    left_expr: Option<Arc<dyn PhysicalExpr>>,
    right_expr: Option<Arc<dyn Expr>>,
}

pub struct JoinSegmentExec {
    segments: Vec<Segment>,
    /// left (build) side
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the Merge Sort
    right: Arc<dyn ExecutionPlan>,
    /// pair of common columns used to join on
    on_left: String,
    on_right: String,
    /// The schema once the join is applied
    left_schema: Option<SchemaRef>,
    take_left_cols: Option<Vec<usize>>,
    right_schema: Option<SchemaRef>,
    take_right_cols: Option<Vec<usize>>,
    schema: SchemaRef,
}

impl JoinSegmentExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        segments: Vec<Segment>,
        left_schema: Option<SchemaRef>,
        right_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let schema = match (&left_schema, &right_schema) {
            (Some(l), Some(r)) => Arc::new(build_join_schema(&l, &r, on)),
            (Some(l), None) => l.clone(),
            (None, Some(r)) => r.clone(),
            (None, None) => DataFusionError::Execution("empty schema".to_string())
        };

        let take_left_cols = if let Some(schema) = &left_schema {
            let batch_schema = left.schema();
            Some(schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, f)|
                    match batch_schema.column_with_name(f.name()) {
                        None => Err(DataFusionError::Plan(format!("Column {} not found in left expression schema", f.name()))),
                        Some(_) => Ok(idx)
                    })
                .collect::<Result<Vec<usize>>>()?)
        } else {
            None
        };

        let take_right_cols = if let Some(schema) = &right_schema {
            let batch_schema = right.schema();
            Some(schema
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, f)|
                    match batch_schema.column_with_name(f.name()) {
                        None => Err(DataFusionError::Plan(format!("Column {} not found in right expression schema", f.name()))),
                        Some(_) => Ok(idx)
                    })
                .collect::<Result<Vec<usize>>>()?)
        } else {
            None
        };

        Ok(JoinSegmentExec {
            segments,
            left,
            right,
            on_left: on.0.to_owned(),
            on_right: on.1.to_owned(),
            left_schema,
            take_left_cols,
            right_schema,
            take_right_cols,
            schema,
        })
    }

    /// left (build) side which gets hashed
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// right (probe) side which are filtered by the hash table
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }

    /// Set of common columns used to join on
    pub fn on(&self) -> &(String, String) {
        &self.on
    }
}

#[async_trait]
impl ExecutionPlan for JoinSegmentExec {
    fn as_any(&self) -> &dyn Any {
        &self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            2 => Ok(Arc::new(JoinSegmentExec::try_new(
                children[0].clone(),
                children[1].clone(),
                &self.on,
                self.segments.clone(),
                self.right_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "JoinSegmentExec wrong number of children".to_string(),
            ))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let left = self.left.execute(partition).await?;
        let right = self.right.execute(partition).await?;

        Ok(Box::pin(JoinSegmentStream {
            segments: self.segments.clone(),
            left_expr_result: vec![],
            left_input: left,
            left_batch: None,
            right_input: right,
            right_batch: None,
            right_idx: 0,
            on_left: self.on_left.clone(),
            on_right: self.on_right.clone(),
            left_schema: self.left_schema.clone(),
            take_left_cols: self.take_left_cols.clone(),
            right_schema: self.right_schema.clone(),
            take_right_cols: self.take_right_cols.clone(),
            left_idx: 0,
            schema: self.schema.clone(),
        }))
    }
}

#[derive(Clone)]
enum CmpResult {
    Eq,
    Lt,
    Gt,
}

struct JoinSegmentStream {
    segments: Vec<Segment>,
    left_expr_result: Vec<Option<ArrayRef>>,
    left_input: SendableRecordBatchStream,
    left_batch: Option<RecordBatch>,
    left_idx: usize,
    right_input: SendableRecordBatchStream,
    right_batch: Option<RecordBatch>,
    right_idx: usize,
    on_left: String,
    on_right: String,
    /// The schema once the join is applied
    left_schema: Option<SchemaRef>,
    take_left_cols: Option<Vec<usize>>,
    right_schema: Option<SchemaRef>,
    take_right_cols: Option<Vec<usize>>,
    // chunked columns
    schema: SchemaRef,
}

impl RecordBatchStream for JoinSegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl JoinSegmentStream {
    fn evaluate_partition(&self, right_span: &Span, output: &mut Vec<Vec<ArrayRef>>) -> Result<()> {
        let shrinked_buffer = if self.right_expr.is_some() || self.take_right_cols.is_some() {
            right_span.shrink_buffers()?
        } else {
            vec![]
        };

        let mut columns: Vec<Vec<ArrayRef>> = Vec::with_capacity(self.schema.fields().len() + 1);

        for (s_idx, segment) in self.segments.iter().enumerate() {
            if let Some(expr) = &self.left_expr_result[s_idx] {
                let a = expr.as_any().downcast_ref::<BooleanArray>().unwrap();
                if !a.value(self.left_idx) {
                    continue;
                }
            }

            if let Some(expr) = &segment.right_expr {
                if !expr.evaluate(&shrinked_buffer)? {
                    continue;
                }
            }

            columns.push(vec![ScalarValue::Utf8(Some(segment.name.clone())).to_array_of_size(right_span.len)]);
            if let Some(ids) = &self.take_left_cols {
                for (cidx, idx) in ids.iter().enumerate() {
                    let batch_col = &self.left_batch.unwrap().columns()[*idx];
                    let col = ScalarValue::try_from_array(batch_col, self.left_idx)?.to_array_of_size(right_span.len);
                    output[cidx].push(col);
                }
            }


            if let Some(ids) = &self.take_right_cols {
                for (cidx, idx) in ids.iter().enumerate() {
                    for batch in shrinked_buffer.iter() {
                        output[cidx].push(batch.columns()[*idx].clone())
                    }
                }
            }
        }

        Ok(())
    }
}

pub struct Span {
    is_processing: bool,
    start_idx: usize,
    end_idx: usize,
    len: usize,
    buffer: Vec<RecordBatch>,
}

impl Span {
    fn new() -> Self {
        Span {
            is_processing: false,
            start_idx: 0,
            end_idx: 0,
            len: 0,
            buffer: vec![],
        }
    }

    fn shrink_buffers(&self) -> Result<Vec<RecordBatch>> {
        self.buffer.iter().enumerate().map(|(idx, batch)| {
            let cols = if idx == 0 && (self.start_idx > 0 || self.end_idx > 0) {
                batch.columns().iter().map(|c| c.slice(self.start_idx, self.end_idx - self.start_idx)).collect()
            } else if idx == self.buffer.len() - 1 && self.end_idx > 0 {
                batch.columns().iter().map(|c| c.slice(0, self.end_idx)).collect()
            } else {
                batch.columns().to_vec()
            };

            RecordBatch::try_new(batch.schema().clone(), cols)?
        }).collect::<Result<Vec<RecordBatch>>>()
    }
}

impl Stream for JoinSegmentStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut span = Span::new();
        let mut to_cmp = false;
        let mut cmp: DynComparator;
        let mut output_buffer: Vec<Vec<ArrayRef>> = Vec::with_capacity(self.schema.fields().len() + 1);
        loop {
            if self.left_batch.is_none() || self.left_idx >= self.left_batch.unwrap().num_rows() {
                match self.left_input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.left_idx = 0;
                        self.left_batch = Some(batch.clone());
                        self.left_expr_result = self.segments.iter().map(|s| {
                            match &s.left_expr {
                                Some(expr) => Some(into_array(expr.evaluate(&batch)?)),
                                None => None,
                            }
                        }).collect();

                        to_cmp = true;
                    }
                    other => return other,
                }
            }

            if self.right_batch.is_none() || self.right_idx >= self.right_batch.unwrap().num_rows() {
                match self.right_input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.right_idx = 0;
                        self.right_batch = Some(batch.clone());
                        span.buffer.push(batch.clone());
                        to_cmp = true;
                    }
                    Poll::Ready(None) => {
                        if span.is_processing {
                            self.evaluate_partition(&span, &mut output_buffer)?;
                            /*{
                                return Poll::Ready(Some(batch.clone()));
                            }*/

                            return Poll::Ready(None);
                        }
                    }
                    other => return other,
                }
            }

            if to_cmp {
                let l = into_array(col(&self.on_left).evaluate(&self.left_batch.unwrap())?);
                let r = into_array(col(&self.on_right).evaluate(&self.right_batch.unwrap())?);
                cmp = arrow::array::build_compare(l.as_ref(), r.as_ref())?;
                to_cmp = false;
            }
            let cmp_result = (cmp)(self.left_idx, self.right_idx);
            match cmp_result {
                std::cmp::Ordering::Equal => {
                    if span.is_processing {
                        span.len += 1;
                    } else {
                        span.is_processing = true;
                        span.start_idx = self.right_idx;
                        span.len = 0;
                    }
                    self.right_idx += 1;
                }
                std::cmp::Ordering::Less | std::cmp::Ordering::Greater => {
                    if span.is_processing {
                        span.is_processing = false;
                        span.end_idx = self.right_idx;
                        self.evaluate_partition(&span)?;
                        /*{
                            return Poll::Ready(Some(is_processing));
                        }*/
                    } else {
                        match cmp_result {
                            std::cmp::Ordering::Less => self.left_idx += 1,
                            std::cmp::Ordering::Greater => self.right_idx += 1,
                            _ => panic!("unexpected")
                        }
                    }
                }
            }
        }
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    on: &JoinOn,
) -> Schema {
    let fields: Vec<Field> = {
        let left_fields = left.fields().iter();

        let right_fields = right
            .fields()
            .iter()
            .filter(|f| !(&on.0 == &on.1 && &on.0 == f.name()));

        // left then right
        left_fields.chain(right_fields).cloned().collect()
    };
    Schema::new(fields)
}