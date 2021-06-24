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
use arrow::array::{ArrayRef, Int8Array, Array, DynComparator, BooleanArray};
use crate::physical_plan::utils::into_array;
use datafusion::logical_plan::Operator;
use crate::expression_tree::multibatch::expr::Expr;
use arrow::compute::kernels;
use std::collections::HashMap;
use arrow::ipc::SchemaBuilder;
use std::borrow::BorrowMut;

pub type JoinOn = (String, String);

pub struct JoinSegmentExec {
    left_expr: Option<Arc<dyn PhysicalExpr>>,
    right_expr: Option<Arc<dyn Expr>>,
    /// left (build) side
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the Merge Sort
    right: Arc<dyn ExecutionPlan>,
    /// pair of common columns used to join on
    on_left: String,
    on_right: String,
    /// The schema once the join is applied
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    schema: SchemaRef,
}

impl JoinSegmentExec {
    pub fn try_new_children(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        left_expr: Option<Arc<dyn PhysicalExpr>>,
        right_expr: Option<Arc<dyn Expr>>,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Result<Self> {
        Ok(JoinSegmentExec {
            left_expr,
            right_expr,
            left,
            right,
            on_left: on.0.to_owned(),
            on_right: on.1.to_owned(),
            left_schema,
            right_schema,
            schema: Arc::new(build_join_schema(left_schema.as_ref(), right_schema.as_ref(), on)),
        })
    }
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        left_expr: Option<Arc<dyn PhysicalExpr>>,
        right_expr: Option<Arc<dyn Expr>>,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
    ) -> Result<Self> {
        let on = (on.0.clone(), on.1.clone());

        Ok(JoinSegmentExec {
            left_expr,
            right_expr,
            left,
            right,
            on_left: on.0.to_owned(),
            on_right: on.1.to_owned(),
            left_schema,
            right_schema,
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
            2 => Ok(Arc::new(JoinSegmentExec::try_new_children(
                children[0].clone(),
                children[1].clone(),
                &self.on,
                self.left_expr.clone(),
                self.right_expr.clone(),
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
            left_expr: self.left_expr.clone(),
            left_input: left,
            left_batch: None,
            right_expr: self.right_expr.clone(),
            right_input: right,
            right_batch: None,
            right_idx: 0,
            on_left: self.on_left.clone(),
            on_right: self.on_right.clone(),
            schema: SchemaBuilder::self.left_schema.,
            right_schema: self.right_schema.clone(),
            left_idx: 0,
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
    left_expr: Option<Arc<dyn PhysicalExpr>>,
    left_expr_result: Option<ArrayRef>,
    left_input: SendableRecordBatchStream,
    left_batch: Option<RecordBatch>,
    left_idx: usize,
    right_expr: Option<Arc<dyn Expr>>,
    right_input: SendableRecordBatchStream,
    right_batch: Option<RecordBatch>,
    right_idx: usize,
    on_left: String,
    on_right: String,
    /// The schema once the join is applied
    schema: SchemaRef,
}

impl RecordBatchStream for JoinSegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn concat_batches(take_cols: &[usize], from_id: usize, to_id: usize, batches: &[RecordBatch]) -> Vec<ArrayRef> {
    match batches.len() {
        1 => {
            take_cols.iter().map(|x| (*x, batches[0][*x].slice(from_id, to_id - from_id))).collect()
        }
        _ => {
            take_cols.iter().map(|col_id| {
                let cols: Vec<dyn Array> = batches.iter().map(|x| {
                    if col_id == 0 {
                        x.columns()[*col_id].slice(from_id, x.num_rows() - from_id)
                    } else if col_id == batches.len() - 1 {
                        x.columns()[*col_id].slice(0, to_id + 1)
                    } else {
                        x.columns()[*col_id].clone()
                    }
                }).collect();

                kernels::concat::concat(&cols).unwrap()
            }).collect()
        }
    }
}

enum ColKind {
    Expr,
    Take,
}

impl JoinSegmentStream {
    fn evaluate_partition(&self, span: &Span) -> Result<Option<RecordBatch>> {
        if let Some(expr) = &self.left_expr_result {
            let a = expr.as_any().downcast_ref::<BooleanArray>().unwrap();
            if !a.value(self.left_idx) {
                return Ok(None);
            }
        }

        if let Some(expr) = &self.right_expr {
            if !expr.evaluate(&span.buffer)? {
                return Ok(None);
            }
        }


        let mut final_cols: Vec<Option<ArrayRef>> = vec![None; self.right_schema.fields().len()];
        let mut expr_cols: Vec<ArrayRef> = vec![];


        if let Some((expr, _)) = &self.right_expr {
            let take_cols = right_cols.iter().enumerate().filter_map(|(i, x)| {
                return if x.0.contains(&ColKind::Expr) {
                    Some(i)
                } else {
                    None
                };
            }).collect();
            expr_cols = concat_batches(&take_cols, from_idx, to_idx, right_batches);
            if !expr.evaluate(&expr_cols.iter().map(|x| x.1).collect(), self.right_idx) {
                return None;
            }
            let mut r: usize = 0;
            right_cols.iter().enumerate().for_each(|(i, x)| {
                if x.0.contains(&ColKind::Take) && x.0.contains(&ColKind::Expr) {
                    final_cols[x.1] = Some(expr_cols[r].clone());
                    r += 1;
                }
            });
        }

        right_cols.iter().
        let left_schema = self.left_schema.fields().iter().map(|x| x.).collect();
        let right_schema = self.right_schema.fields().iter().map(|x| x.).collect();
        let batch = RecordBatch::try_new(self.left_schema.)
        None
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
}

impl Stream for JoinSegmentStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut span = Span::new();
        let mut to_cmp = false;
        let mut cmp: DynComparator;

        loop {
            if self.left_batch.is_none() || self.left_idx >= self.left_batch.unwrap().num_rows() {
                match self.left_input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.left_idx = 0;
                        self.left_batch = Some(batch.clone());
                        if let Some(expr) = &self.left_expr {
                            self.left_expr_result = Some(into_array(expr.evaluate(&batch)?));
                            left_expr_result = Some(self.left_expr_result.unwrap().as_any().downcast_ref::<BooleanArray>().unwrap())
                        }
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
                        right_buffer.push(batch.clone());
                        to_cmp = true;
                    }
                    Poll::Ready(None) => {
                        if is_processing {
                            if let Some(batch) = self.evaluate_partition() {
                                return Poll::Ready(Some(batch.clone()));
                            }

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
                    if is_processing {
                        partition_len += 1;
                    } else {
                        is_processing = true;
                        partition_start_idx = self.right_idx;
                        partition_len = 0;
                    }
                    self.right_idx += 1;
                }
                std::cmp::Ordering::Less | std::cmp::Ordering::Greater => {
                    if is_processing {
                        is_processing = false;
                        partition_end_idx = self.right_idx;
                        if let Some(batch) = self.evaluate_partition() {
                            return Poll::Ready(Some(is_processing));
                        }
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