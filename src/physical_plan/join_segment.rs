use std::sync::Arc;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, RecordBatchStream, ColumnarValue, PhysicalExpr};
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use std::any::Any;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::merge::MergeExec;
use futures::{Stream, StreamExt, TryStreamExt, TryStream};
use std::task::{Context, Poll};
use std::pin::Pin;
use arrow::record_batch::RecordBatch;
use arrow::error::{Result as ArrowResult, ArrowError};
use datafusion::physical_plan::expressions::col;
use arrow::array::{ArrayRef, Int8Array, Array, DynComparator, BooleanArray, StringArray, MutableArrayData, build_compare};
use crate::physical_plan::utils::into_array;
use datafusion::logical_plan::Operator;
use crate::expression_tree::multibatch::expr::Expr;
use arrow::compute::kernels;
use std::collections::HashMap;
use arrow::ipc::SchemaBuilder;
use std::borrow::{BorrowMut, Borrow};
use datafusion::scalar::ScalarValue;
use arrow::buffer::MutableBuffer;
use std::ops::Deref;
use std::cmp::Ordering;
use datafusion::physical_plan::common;
use std::cell::RefCell;

pub type JoinOn = (String, String);

#[derive(Debug, Clone)]
pub struct Segment {
    name: String,
    left_expr: Option<Arc<dyn PhysicalExpr>>,
    right_expr: Option<Arc<dyn Expr>>,
}

impl Segment {
    fn new(name: &str, left_expr: Option<Arc<dyn PhysicalExpr>>, right_expr: Option<Arc<dyn Expr>>) -> Self {
        Segment {
            name: name.to_string(),
            left_expr,
            right_expr,
        }
    }
}

#[derive(Debug)]
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
            (Some(l), Some(r)) => Ok(Arc::new(build_join_schema(&l, &r, on))),
            (Some(l), None) => Ok(l.clone()),
            (None, Some(r)) => Ok(r.clone()),
            (None, None) => Err(DataFusionError::Execution("empty schema".to_string()))
        }?;

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
}

#[async_trait]
impl ExecutionPlan for JoinSegmentExec {
    fn as_any(&self) -> &dyn Any {
        self
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
                &(self.on_left.clone(), self.on_right.clone()),
                self.segments.clone(),
                self.left_schema.clone(),
                self.right_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "JoinSegmentExec wrong number of children".to_string(),
            ))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let mut left = self.left.execute(partition).await?;
        let mut right = self.right.execute(partition).await?;

        let mut left_batch: Option<RecordBatch> = None;
        let mut right_batch: Option<RecordBatch> = None;

        match left.try_next().await? {
            None => return Ok(Box::pin(EmptyJoinSegmentStream::new(self.schema.clone()))),
            Some(b) => {
                left_batch = Some(b.clone());

                match right.try_next().await? {
                    Name => return Ok(Box::pin(EmptyJoinSegmentStream::new(self.schema.clone()))),
                    Some(b) => right_batch = Some(b.clone()),
                }
            }
        }

        Ok(Box::pin(JoinSegmentStream {
            segments: self.segments.clone(),
            left_expr_result: vec![],
            left_input: left,
            left_batch: left_batch.clone().unwrap(),
            left_idx: 0,
            right_input: right,
            right_batch: right_batch.clone().unwrap(),
            right_idx: 0,
            on_left: self.on_left.clone(),
            on_right: self.on_right.clone(),
            left_schema: self.left_schema.clone(),
            take_left_cols: self.take_left_cols.clone(),
            right_schema: self.right_schema.clone(),
            take_right_cols: self.take_right_cols.clone(),
            schema: self.schema.clone(),
        }))
    }
}

struct EmptyJoinSegmentStream {
    schema: SchemaRef,
}

impl EmptyJoinSegmentStream {
    fn new(schema: SchemaRef) -> Self {
        EmptyJoinSegmentStream { schema }
    }
}

impl RecordBatchStream for EmptyJoinSegmentStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyJoinSegmentStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

struct JoinSegmentStream {
    segments: Vec<Segment>,
    left_expr_result: Vec<Option<ArrayRef>>,
    left_input: SendableRecordBatchStream,
    left_batch: RecordBatch,
    left_idx: usize,
    right_input: SendableRecordBatchStream,
    right_batch: RecordBatch,
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
        let shrinked_buffer = right_span.shrink_buffers()?;

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
                    let batch_col = self.left_batch.columns()[*idx].clone();
                    let col = ScalarValue::try_from_array(&batch_col, self.left_idx)?.to_array_of_size(right_span.len);
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

    fn shrink_buffers(&self) -> ArrowResult<Vec<RecordBatch>> {
        self.buffer.iter().enumerate().map(|(idx, batch)| {
            let cols = if idx == 0 && (self.start_idx > 0 || self.end_idx > 0) {
                batch.columns().iter().map(|c| c.slice(self.start_idx, self.end_idx - self.start_idx)).collect()
            } else if idx == self.buffer.len() - 1 && self.end_idx > 0 {
                batch.columns().iter().map(|c| c.slice(0, self.end_idx)).collect()
            } else {
                batch.columns().to_vec()
            };

            RecordBatch::try_new(batch.schema().clone(), cols)
        }).collect::<ArrowResult<Vec<RecordBatch>>>()
    }
}

fn concat_batches(schema: SchemaRef, output_buffer: &Vec<Vec<ArrayRef>>) -> ArrowResult<RecordBatch> {
    let cols = output_buffer.
        iter().
        map(|chunks| kernels::concat::concat(&chunks.iter().map(|c| c.as_ref()).collect::<Vec<_>>())).
        collect::<ArrowResult<Vec<ArrayRef>>>()?;
    RecordBatch::try_new(schema.clone(), cols)
}


impl Stream for JoinSegmentStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut span = Span::new();
        let mut output_buffer: Vec<Vec<ArrayRef>> = Vec::new();


        loop {
            let mut left_cmp_col = into_array(col(&self.on_left).evaluate(&self.left_batch).or_else(|e| Err(e.into_arrow_external_error()))?);
            let mut right_cmp_col = into_array(col(&self.on_right).evaluate(&self.right_batch).or_else(|e| Err(e.into_arrow_external_error()))?);
            let mut cmp = match build_compare(left_cmp_col.as_ref(), right_cmp_col.as_ref()) {
                Ok(a) => a,
                Err(err) => return Poll::Ready(Some(Err(err))),
            };
            let mut to_cmp = false;


            'inner: loop {
                if self.left_idx >= self.left_batch.num_rows() {
                    match self.left_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.left_idx = 0;
                            self.left_batch = batch.clone();
                            let res = self.segments.iter().map(|s| {
                                match &s.left_expr {
                                    Some(expr) => {
                                        match expr.evaluate(&batch) {
                                            Ok(cv) => Ok(Some(into_array(cv))),
                                            Err(err) => Err(err)
                                        }
                                    }
                                    None => Ok(None),
                                }
                            }).collect::<Result<Vec<Option<ArrayRef>>>>();

                            match res {
                                Ok(res) => self.left_expr_result = res,
                                Err(err) => return Poll::Ready(Some(Err(err.into_arrow_external_error())))
                            }

                            to_cmp = true;

                        }
                        other => return other,
                    }
                }

                if self.right_idx >= self.right_batch.num_rows() {
                    match self.right_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.right_idx = 0;
                            self.right_batch = batch.clone();
                            span.buffer.push(batch.clone());

                            to_cmp = true;
                        }
                        Poll::Ready(None) => {
                            return if span.is_processing {
                                match self.evaluate_partition(&span, &mut output_buffer) {
                                    Err(err) => return Poll::Ready(Some(Err(err.into_arrow_external_error()))),
                                    _ => {}
                                }

                                if !output_buffer.is_empty() {
                                    Poll::Ready(Some(concat_batches(self.schema.clone(), &output_buffer)))
                                } else {
                                    Poll::Ready(None)
                                }
                            } else {
                                Poll::Ready(None)
                            };
                        }
                        other => return other,
                    }
                }

                if to_cmp {
                    break 'inner;
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
                            match self.evaluate_partition(&span, &mut output_buffer) {
                                Err(err) => return Poll::Ready(Some(Err(err.into_arrow_external_error()))),
                                _ => {}
                            }
                            return if output_buffer.len() > 1000 {
                                Poll::Ready(Some(concat_batches(self.schema.clone(), &output_buffer)))
                            } else {
                                Poll::Ready(None)
                            };
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

#[cfg(test)]
mod tests {
    use datafusion::{
        error::{Result},
    };
    use datafusion::physical_plan::{ExecutionPlan, common};
    use std::sync::Arc;
    use datafusion::physical_plan::memory::MemoryExec;
    use crate::physical_plan::join_segment::{JoinSegmentExec, Segment};
    use datafusion::physical_plan::expressions::{Column, Literal, BinaryExpr};
    use datafusion::scalar::ScalarValue;
    use datafusion::logical_plan::Operator;
    use crate::expression_tree::multibatch::count::Count;
    use crate::expression_tree::boolean_op::Gt;
    use arrow::record_batch::RecordBatch;
    use arrow::datatypes::{Schema, DataType, Field};
    use arrow::ipc::BoolArgs;
    use arrow::array::{BooleanArray, Int32Array};

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<bool>),
        c: (&str, &Vec<bool>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    pub fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<bool>),
        c: (&str, &Vec<bool>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Boolean, false),
            Field::new(c.0, DataType::Boolean, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(BooleanArray::from(b.1.clone())),
                Arc::new(BooleanArray::from(c.1.clone())),
            ],
        )
            .unwrap()
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let left_plan = build_table(
            ("u1", &vec![1, 2, 3]),
            ("a1", &vec![true, false, false]), // 7 does not exist on the right
            ("b1", &vec![true, false, false]),
        );
        let right_plan = build_table(
            ("u2", &vec![1, 2, 3]),
            ("a2", &vec![true, false, false]),
            ("b2", &vec![true, false, false]),
        );
        let on = &("a1".to_string(), "a2".to_string());

        let s1 = {
            let left_expr = {
                let left = Column::new("a1");
                let right = Literal::new(ScalarValue::Boolean(Some(true)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let right_expr = {
                let lhs = Column::new("a2");
                let rhs = Literal::new(ScalarValue::Boolean(Some(true)));
                let op = BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs));
                Count::<Gt>::try_new(&right_plan.schema(), Arc::new(op), 0)?
            };


            Segment::new("a", Some(Arc::new(left_expr)), Some(Arc::new(right_expr)))
        };

        let s2 = {
            let left_expr = {
                let lhs = Column::new("b1");
                let rhs = Literal::new(ScalarValue::Boolean(Some(true)));
                BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs))
            };

            let right_expr = {
                let lhs = Column::new("b2");
                let rhs = Literal::new(ScalarValue::Boolean(Some(true)));
                let op = BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs));
                Count::<Gt>::try_new(&right_plan.schema(), Arc::new(op), 0)?
            };


            Segment::new("a", Some(Arc::new(left_expr)), Some(Arc::new(right_expr)))
        };

        let left_schema = Schema::new(vec![
            Field::new("segment", DataType::Utf8, false),
            Field::new("a1", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![
            Field::new("b2", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let join = JoinSegmentExec::try_new(
            left_plan,
            right_plan,
            on,
            vec![s1, s2],
            Some(Arc::new(left_schema)),
            Some(Arc::new(right_schema)),
        )?;

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        println!("{}", arrow::util::pretty::pretty_format_batches(&batches)?);
        Ok(())
    }
}