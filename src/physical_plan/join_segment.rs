use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, RecordBatchStream, ColumnarValue};
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
use arrow::array::{ArrayRef, Int8Array, Array};
use crate::physical_plan::utils::into_array;
use datafusion::logical_plan::Operator;
use crate::expression_tree::expr::Expr;
use arrow::compute::kernels;

pub type JoinOn = (String, String);

pub struct JoinSegmentExec {
    left_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
    right_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
    /// left (build) side
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the Merge Sort
    right: Arc<dyn ExecutionPlan>,
    /// pair of common columns used to join on
    on_left: String,
    on_right: String,
    /// The schema once the join is applied
    schema: SchemaRef,
}

impl JoinSegmentExec {
    pub fn try_new_children(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        left_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
        right_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = Arc::new(build_join_schema(
            &left_schema,
            &right_schema,
            on,
        ));

        let on = (on.0.clone(), on.1.clone());

        Ok(JoinSegmentExec {
            left_expr,
            right_expr,
            left,
            right,
            on_left: on.0.to_owned(),
            on_right: on.1.to_owned(),
            schema,
        })
    }
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: &JoinOn,
        left_expr: Option<(Arc<dyn Expr<bool>>, SchemaRef)>,
        right_expr: Option<(Arc<dyn Expr<bool>>, SchemaRef)>,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = Arc::new(build_join_schema(
            &left_schema,
            &right_schema,
            on,
        ));

        let on = (on.0.clone(), on.1.clone());

        let left_expr = left_expr.map(|(expr, s)| {
            (expr, left_schema.fields().iter().filter_map(|f| s.index_of(f.name()).ok()).collect())
        });

        let right_expr = right_expr.map(|(expr, s)| {
            (expr, right_schema.fields().iter().filter_map(|f| s.index_of(f.name()).ok()).collect())
        });

        Ok(JoinSegmentExec {
            left_expr,
            right_expr,
            left,
            right,
            on_left: on.0.to_owned(),
            on_right: on.1.to_owned(),
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
            left_join_predicate: None,
            right_expr: self.right_expr.clone(),
            right_input: right,
            right_batch: None,
            right_join_predicate: None,
            right_idx: 0,
            on_left: self.on_left.clone(),
            on_right: self.on_right.clone(),
            schema: self.schema.clone(),
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
    left_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
    left_input: SendableRecordBatchStream,
    left_batch: Option<RecordBatch>,
    left_join_predicate: Option<ArrayRef>,
    left_idx: usize,
    right_expr: Option<(Arc<dyn Expr<bool>>, Vec<usize>)>,
    right_input: SendableRecordBatchStream,
    right_batch: Option<RecordBatch>,
    right_join_predicate: Option<ArrayRef>,
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

impl JoinSegmentStream {
    fn compare_predicates(self) -> CmpResult {
        let left = self.left_join_predicate.unwrap();
        let right = self.right_join_predicate.unwrap();

        match left.data_type() {
            DataType::Int8 => {
                let l = left.as_any().downcast_ref::<Int8Array>().unwrap();
                let r = right.as_any().downcast_ref::<Int8Array>().unwrap();
                if l.value(self.left_idx) == r.value(self.right_idx) {
                    CmpResult::Eq
                } else if l.value(self.left_idx) < r.value(self.right_idx) {
                    CmpResult::Lt
                } else {
                    CmpResult::Gt
                }
            }
            _ => panic!("unimplemented")
        }
    }
}

fn concat_batches(take_cols: Vec<usize>, from_id: usize, to_id: usize, batches: Vec<RecordBatch>) -> Vec<ArrayRef> {
    match batches.len() {
        1 => {
            take_cols.iter().map(|x| batches[0][*x].slice(from_id, to_id - from_id)).collect()
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

impl Stream for JoinSegmentStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut is_processing = false;
        let mut partition_start_idx: usize = 0;
        let mut partition_end_idx: usize = 0;
        let mut partition_len: usize = 0;
        let mut right_buffer: Vec<RecordBatch> = vec![];

        loop {
            if self.left_batch.is_none() || self.left_idx >= self.left_batch.unwrap().num_rows() {
                match self.left_input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.left_idx = 0;
                        self.left_batch = Some(batch.clone());
                        self.left_join_predicate = Some(into_array(col(&self.on_left).evaluate(&batch)?));
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
                        self.right_join_predicate = Some(into_array(col(&self.on_right).evaluate(&batch)?));
                    }
                    Poll::Ready(None) => {
                        if is_processing {
                            // todo: return of unfinished user
                        }
                    }
                    other => return other,
                }
            }

            let res = self.compare_predicates();
            match res {
                CmpResult::Eq => {
                    if is_processing {
                        partition_len += 1;
                    } else {
                        is_processing = true;
                        partition_start_idx = self.right_idx;
                        partition_len = 0;
                    }
                    self.right_idx += 1;
                }
                CmpResult::Lt | CmpResult::Gt => {
                    if is_processing {
                        is_processing = false;
                        partition_end_idx = self.right_idx;
                        return Poll::Ready(concat_batches());
                    }

                    match res {
                        CmpResult::Lt => self.left_idx += 1,
                        CmpResult::Gt => self.right_idx += 1,
                        _ => panic!("unexpected")
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