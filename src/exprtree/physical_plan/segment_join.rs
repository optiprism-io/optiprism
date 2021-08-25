use crate::exprtree::segment::expressions::multibatch::expr::Expr;
use crate::exprtree::utils::into_array;
use arrow::array::{
    build_compare, Array, ArrayRef, BooleanArray, DynComparator, Int8Array, MutableArrayData,
    StringArray,
};
use arrow::buffer::MutableBuffer;
use arrow::compute::kernels;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::SchemaBuilder;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::Operator;
use datafusion::physical_plan::common;
use datafusion::physical_plan::expressions::{col, Column};
use datafusion::physical_plan::{
    ColumnarValue, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt, TryStream, TryStreamExt};
use std::any::Any;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub type JoinOn = (Column, Column);

#[derive(Debug, Clone)]
pub struct Segment {
    pub left_expr: Option<Arc<dyn PhysicalExpr>>,
    pub right_expr: Option<Arc<dyn Expr>>,
}

impl Segment {
    fn new(left_expr: Option<Arc<dyn PhysicalExpr>>, right_expr: Option<Arc<dyn Expr>>) -> Self {
        Segment {
            left_expr,
            right_expr,
        }
    }
}

#[derive(Debug)]
pub struct JoinExec {
    segments: Vec<Segment>,
    /// left (build) side
    left: Arc<dyn ExecutionPlan>,
    /// right (probe) side which are filtered by the Merge Sort
    right: Arc<dyn ExecutionPlan>,
    /// pair of common columns used to join on
    on: JoinOn,
    /// The schema once the join is applied
    take_left_cols: Option<Vec<usize>>,
    take_right_cols: Option<Vec<usize>>,
    schema: SchemaRef,
    segment_col: bool,
    target_batch_size: usize,
}

impl JoinExec {
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        segments: Vec<Segment>,
        schema: SchemaRef,
        target_batch_size: usize,
    ) -> Result<Self> {
        if schema.fields().len() == 0 {
            return Err(DataFusionError::Plan("empty schema".to_string()));
        }

        let mut take_left_cols: Vec<usize> = vec![];
        let mut take_right_cols: Vec<usize> = vec![];
        for field in schema.fields().iter() {
            if field.name().eq("segment") {
                continue;
            }
            match left.schema().index_of(field.name()) {
                Ok(idx) => take_left_cols.push(idx),
                _ => match right.schema().index_of(field.name()) {
                    Ok(idx) => take_right_cols.push(idx),
                    Err(_) => {
                        return Err(DataFusionError::Plan(format!(
                            "Column {} not found",
                            field.name()
                        )))
                    }
                },
            }
        }

        Ok(JoinExec {
            segments,
            left,
            right,
            on: on.clone(),
            take_left_cols: if take_left_cols.is_empty() {
                None
            } else {
                Some(take_left_cols)
            },
            take_right_cols: if take_right_cols.is_empty() {
                None
            } else {
                Some(take_right_cols)
            },
            schema: schema.clone(),
            segment_col: schema.fields()[0].name().eq("segment"),
            target_batch_size,
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
impl ExecutionPlan for JoinExec {
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

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            2 => Ok(Arc::new(JoinExec::try_new(
                children[0].clone(),
                children[1].clone(),
                self.on.clone(),
                self.segments.clone(),
                self.schema.clone(),
                self.target_batch_size,
            )?)),
            _ => Err(DataFusionError::Internal(
                "JoinSegmentExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        let mut left = self.left.execute(partition).await?;
        let mut right = self.right.execute(partition).await?;

        let mut left_batch: Option<RecordBatch> = None;
        let mut right_batch: Option<RecordBatch> = None;

        match left.try_next().await? {
            None => {
                return Ok(Box::pin(EmptyJoinStream::new(self.schema.clone())));
            }
            Some(b) => {
                left_batch = Some(b.clone());

                match right.try_next().await? {
                    None => return Ok(Box::pin(EmptyJoinStream::new(self.schema.clone()))),
                    Some(b) => right_batch = Some(b.clone()),
                }
            }
        }

        let mut ret = JoinStream {
            segments: self.segments.clone(),
            left_expr_result: vec![],
            left_input: left,
            left_batch: left_batch.clone().unwrap(),
            left_idx: 0,
            right_input: right,
            right_batch: right_batch.clone().unwrap(),
            right_idx: 0,
            on: self.on.clone(),
            take_left_cols: self.take_left_cols.clone(),
            take_right_cols: self.take_right_cols.clone(),
            schema: self.schema.clone(),
            segment_names: self.segment_col,
            target_batch_size: self.target_batch_size,
        };

        ret.evaluate_left_expr()?;

        Ok(Box::pin(ret))
    }
}

struct EmptyJoinStream {
    schema: SchemaRef,
}

impl EmptyJoinStream {
    fn new(schema: SchemaRef) -> Self {
        EmptyJoinStream { schema }
    }
}

impl RecordBatchStream for EmptyJoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyJoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

struct JoinStream {
    segments: Vec<Segment>,
    left_expr_result: Vec<Option<ArrayRef>>,
    left_input: SendableRecordBatchStream,
    left_batch: RecordBatch,
    left_idx: usize,
    right_input: SendableRecordBatchStream,
    right_batch: RecordBatch,
    right_idx: usize,
    on: JoinOn,
    /// The schema once the join is applied
    take_left_cols: Option<Vec<usize>>,
    take_right_cols: Option<Vec<usize>>,
    // chunked columns
    schema: SchemaRef,
    segment_names: bool,
    target_batch_size: usize,
}

impl RecordBatchStream for JoinStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl JoinStream {
    fn evaluate_left_expr(&mut self) -> Result<()> {
        let expr_result = self
            .segments
            .iter()
            .map(|s| match &s.left_expr {
                Some(expr) => match expr.evaluate(&self.left_batch) {
                    Ok(cv) => Ok(Some(into_array(cv))),
                    Err(err) => Err(err),
                },
                None => Ok(None),
            })
            .collect::<Result<Vec<Option<ArrayRef>>>>();

        self.left_expr_result = match expr_result {
            Ok(res) => res,
            Err(err) => return Err(err),
        };
        Ok(())
    }

    fn evaluate_partition(&self, right_span: &Span, output: &mut OutputBuffer) -> Result<()> {
        let shrinked_buffer = right_span.record_batches()?;
        for (idx, segment) in self.segments.iter().enumerate() {
            if let Some(expr) = &self.left_expr_result[idx] {
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

            let mut offset: usize = 0;
            if self.segment_names {
                output.push_to_column(
                    0,
                    ScalarValue::Int8(Some(idx as i8)).to_array_of_size(right_span.len),
                );
                offset += 1;
            }
            if let Some(ids) = &self.take_left_cols {
                for (dst_idx, src_idx) in ids.iter().enumerate() {
                    let batch_col = self.left_batch.columns()[*src_idx].clone();
                    let col = ScalarValue::try_from_array(&batch_col, self.left_idx)?
                        .to_array_of_size(right_span.len);
                    output.push_to_column(dst_idx + offset, col.clone());
                }
                offset += ids.len();
            }

            if let Some(ids) = &self.take_right_cols {
                for (dst_idx, src_idx) in ids.iter().enumerate() {
                    for batch in shrinked_buffer.iter() {
                        output.push_to_column(dst_idx + offset, batch.columns()[*src_idx].clone())
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
    len: usize,
    buffer: Vec<RecordBatch>,
}

impl Span {
    fn new() -> Self {
        Span {
            is_processing: false,
            start_idx: 0,
            len: 0,
            buffer: vec![],
        }
    }

    fn record_batches(&self) -> ArrowResult<Vec<RecordBatch>> {
        let mut resp: Vec<RecordBatch> = vec![];
        let mut row_id = self.start_idx;
        let mut r_len = self.len;
        for batch in self.buffer.iter() {
            if row_id + r_len > batch.columns()[0].len() {
                if row_id == 0 {
                    resp.push(batch.clone())
                } else {
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| c.slice(row_id, c.len() - row_id))
                        .collect();
                    resp.push(RecordBatch::try_new(self.buffer[0].schema().clone(), cols)?);
                }
                r_len -= batch.columns()[0].len() - row_id;
                row_id = 0;
            } else {
                if row_id == 0 && r_len == batch.columns()[0].len() {
                    resp.push(batch.clone())
                } else {
                    let cols = batch
                        .columns()
                        .iter()
                        .map(|c| c.slice(row_id, r_len))
                        .collect();
                    resp.push(RecordBatch::try_new(self.buffer[0].schema().clone(), cols)?);
                }
                break;
            }
        }

        Ok(resp)
    }
}

fn make_record_batch(schema: SchemaRef, output_buffer: &OutputBuffer) -> ArrowResult<RecordBatch> {
    let cols = if output_buffer.num_chunks == 1 {
        output_buffer
            .columns()
            .iter()
            .map(|chunks| chunks[0].clone())
            .collect::<Vec<ArrayRef>>()
    } else {
        output_buffer
            .columns()
            .iter()
            .map(|chunks| {
                kernels::concat::concat(&chunks.iter().map(|c| c.as_ref()).collect::<Vec<_>>())
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?
    };

    RecordBatch::try_new(schema.clone(), cols)
}

struct OutputBuffer {
    columns: Vec<Vec<ArrayRef>>,
    len: usize,
    num_chunks: usize,
}

impl OutputBuffer {
    fn new(schema: &Schema) -> Self {
        OutputBuffer {
            columns: vec![Vec::new(); schema.fields().len()],
            len: 0,
            num_chunks: 0,
        }
    }

    fn push_to_column(&mut self, col_id: usize, v: ArrayRef) {
        self.columns[col_id].push(v.clone());
        if col_id == 0 {
            self.len += v.len();
            self.num_chunks += 1;
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn columns(&self) -> &[Vec<ArrayRef>] {
        &self.columns[..]
    }
}

impl Stream for JoinStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut span = Span::new();
        let mut output_buffer = OutputBuffer::new(&self.schema);

        loop {
            let mut left_cmp_col = into_array(
                self.on
                    .0
                    .evaluate(&self.left_batch)
                    .or_else(|e| Err(e.into_arrow_external_error()))?,
            );
            let mut right_cmp_col = into_array(
                self.on
                    .1
                    .evaluate(&self.right_batch)
                    .or_else(|e| Err(e.into_arrow_external_error()))?,
            );
            let mut cmp = match build_compare(left_cmp_col.as_ref(), right_cmp_col.as_ref()) {
                Ok(a) => a,
                Err(err) => return Poll::Ready(Some(Err(err))),
            };
            let mut to_cmp = false;

            'inner: loop {
                if self.right_idx >= self.right_batch.num_rows() {
                    match self.right_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.right_batch = batch.clone();
                            self.right_idx = 0;
                            span.buffer.push(batch.clone());

                            to_cmp = true;
                        }
                        Poll::Ready(None) => {
                            return if span.is_processing {
                                match self.evaluate_partition(&span, &mut output_buffer) {
                                    Err(err) => {
                                        return Poll::Ready(Some(Err(
                                            err.into_arrow_external_error()
                                        )))
                                    }
                                    _ => {}
                                }

                                if !output_buffer.is_empty() {
                                    Poll::Ready(Some(make_record_batch(
                                        self.schema.clone(),
                                        &output_buffer,
                                    )))
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

                if self.left_idx >= self.left_batch.num_rows() {
                    match self.left_input.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.left_batch = batch.clone();
                            self.left_idx = 0;
                            match self.evaluate_left_expr() {
                                Err(err) => {
                                    return Poll::Ready(Some(Err(err.into_arrow_external_error())))
                                }
                                _ => {}
                            }

                            to_cmp = true;
                        }
                        other => {
                            return other;
                        }
                    }
                }

                if to_cmp {
                    break 'inner;
                }

                let lv = left_cmp_col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(self.left_idx);
                let rv = right_cmp_col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(self.right_idx);
                let cmp_result = (cmp)(self.left_idx, self.right_idx);
                match cmp_result {
                    std::cmp::Ordering::Equal => {
                        if span.is_processing {
                            span.len += 1;
                        } else {
                            span.is_processing = true;
                            span.start_idx = self.right_idx;
                            span.start_idx = self.right_idx;
                            span.len = 1;
                            span.buffer = vec![self.right_batch.clone()];
                        }
                        self.right_idx += 1;
                    }
                    std::cmp::Ordering::Less | std::cmp::Ordering::Greater => {
                        if span.is_processing {
                            span.is_processing = false;
                            match self.evaluate_partition(&span, &mut output_buffer) {
                                Err(err) => {
                                    return Poll::Ready(Some(Err(err.into_arrow_external_error())))
                                }
                                _ => {}
                            }

                            if output_buffer.len >= self.target_batch_size {
                                return Poll::Ready(Some(make_record_batch(
                                    self.schema.clone(),
                                    &output_buffer,
                                )));
                            }
                        } else {
                            match cmp_result {
                                std::cmp::Ordering::Less => {
                                    self.left_idx += 1;
                                }
                                std::cmp::Ordering::Greater => {
                                    self.right_idx += 1;
                                }
                                _ => panic!("unexpected"),
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::execution::context::make_context;
    use crate::exprtree::logical_plan::segment_join::{
        JoinPlanNode, Segment as LogicalSegment, SegmentExpr,
    };
    use crate::exprtree::physical_plan::segment_join::{JoinExec, Segment};
    use crate::exprtree::segment::expressions::boolean_op::Gt;
    use crate::exprtree::segment::expressions::multibatch::count::Count;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::BoolArgs;
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    pub use datafusion::error::Result;
    use datafusion::logical_plan::Expr as LogicalExpr;
    use datafusion::logical_plan::{
        Column as LogicalColumn, DFSchema, LogicalPlan, LogicalPlanBuilder, Operator,
    };
    use datafusion::physical_plan::expressions::{col, BinaryExpr, Column, Literal};
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::{common, ExecutionPlan};
    use datafusion::scalar::ScalarValue;
    use std::collections::BTreeMap;
    use std::convert::TryFrom;
    use std::convert::TryInto;
    use std::sync::Arc;

    pub fn build_batches(schema: SchemaRef, input: &Vec<Vec<Vec<i8>>>) -> Vec<RecordBatch> {
        let mut batches: Vec<RecordBatch> = vec![];

        for raw_batch in input.iter() {
            let mut cols: Vec<Vec<i8>> = vec![vec![]; input[0][0].len()];
            for row in raw_batch.iter() {
                for (idx, value) in row.iter().enumerate() {
                    cols[idx].push(*value);
                }
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                cols.iter()
                    .map(|x| Arc::new(Int8Array::from(x.clone())) as ArrayRef)
                    .collect::<Vec<ArrayRef>>(),
            )
            .unwrap();
            batches.push(batch);
        }
        batches
    }

    /*
       +----+----+----+----+----+--------+
       | a1 | b1 | u2 | a2 | b2 |        |
       +----+----+----+----+----+--------+
       | 1  |    |    |    |    | 1      |
       |    |    |    | 1  |    | 2      |
       | 1  |    |    | 1  |    | 1, 2, 3|
       |    | 1  |    |    | 1  | 4      |
       +----+----+----+----+----+--------+
    */

    pub fn users() -> Vec<Vec<Vec<i8>>> {
        vec![
            //       u1 a1 b1
            vec![vec![1, 1, 1], vec![3, 0, 0], vec![4, 1, 0]],
            vec![vec![5, 1, 0]],
            vec![vec![6, 0, 0], vec![7, 1, 0]],
            vec![vec![8, 0, 1], vec![9, 1, 1]],
        ]
    }

    pub fn events() -> Vec<Vec<Vec<i8>>> {
        vec![
            //        u2 a2 b2
            vec![vec![2, 1, 1]],
            vec![vec![2, 1, 1], vec![3, 0, 0], vec![3, 0, 0]],
            vec![vec![4, 0, 0], vec![6, 1, 0]],
            vec![vec![6, 1, 0], vec![6, 1, 0], vec![7, 1, 0]],
            vec![vec![7, 1, 0]],
            vec![vec![8, 0, 1], vec![8, 0, 1]],
            vec![vec![9, 0, 0], vec![9, 1, 0], vec![9, 0, 1]],
            vec![vec![9, 1, 1]],
        ]
    }

    pub fn exp() -> Vec<Vec<Vec<i8>>> {
        vec![
            //       s  u1 a1 b1 a2 b2
            vec![
                vec![0, 4, 1, 0, 0, 0],
                vec![1, 6, 0, 0, 1, 0],
                vec![1, 6, 0, 0, 1, 0],
                vec![1, 6, 0, 0, 1, 0],
            ],
            vec![
                vec![0, 7, 1, 0, 1, 0],
                vec![0, 7, 1, 0, 1, 0],
                vec![1, 7, 1, 0, 1, 0],
                vec![1, 7, 1, 0, 1, 0],
                vec![2, 7, 1, 0, 1, 0],
                vec![2, 7, 1, 0, 1, 0],
            ],
            vec![vec![3, 8, 0, 1, 0, 1], vec![3, 8, 0, 1, 0, 1]],
            vec![
                vec![0, 9, 1, 1, 0, 0],
                vec![0, 9, 1, 1, 1, 0],
                vec![0, 9, 1, 1, 0, 1],
                vec![0, 9, 1, 1, 1, 1],
                vec![1, 9, 1, 1, 0, 0],
                vec![1, 9, 1, 1, 1, 0],
                vec![1, 9, 1, 1, 0, 1],
                vec![1, 9, 1, 1, 1, 1],
                vec![2, 9, 1, 1, 0, 0],
                vec![2, 9, 1, 1, 1, 0],
                vec![2, 9, 1, 1, 0, 1],
                vec![2, 9, 1, 1, 1, 1],
                vec![3, 9, 1, 1, 0, 0],
                vec![3, 9, 1, 1, 1, 0],
                vec![3, 9, 1, 1, 0, 1],
                vec![3, 9, 1, 1, 1, 1],
            ],
        ]
    }

    #[tokio::test]
    async fn test_exec() -> Result<()> {
        let left_plan = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("u1", DataType::Int8, false),
                Field::new("a1", DataType::Int8, false),
                Field::new("b1", DataType::Int8, false),
            ]));
            let batches = build_batches(schema.clone(), &users());
            Arc::new(MemoryExec::try_new(&[batches], schema.clone(), None).unwrap())
        };

        let right_plan = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("u2", DataType::Int8, false),
                Field::new("a2", DataType::Int8, false),
                Field::new("b2", DataType::Int8, false),
            ]));
            let batches = build_batches(schema.clone(), &events());
            Arc::new(MemoryExec::try_new(&[batches], schema.clone(), None).unwrap())
        };

        let s1 = {
            let left_expr = {
                let lhs = Column::new_with_schema("a1", &left_plan.schema())?;
                let rhs = Literal::new(ScalarValue::Int8(Some(1)));
                BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs))
            };

            Segment::new(Some(Arc::new(left_expr)), None)
        };

        let s2 = {
            let right_expr = {
                let lhs = Column::new_with_schema("a2", &right_plan.schema())?;
                let rhs = Literal::new(ScalarValue::Int8(Some(1)));
                let op = BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs));
                Count::<Gt>::try_new(&right_plan.schema(), Arc::new(op), 0)?
            };

            Segment::new(None, Some(Arc::new(right_expr)))
        };

        let s3 = {
            let left_expr = {
                let left = Column::new_with_schema("a1", &left_plan.schema())?;
                let right = Literal::new(ScalarValue::Int8(Some(1)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let right_expr = {
                let lhs = Column::new_with_schema("a2", &right_plan.schema())?;
                let rhs = Literal::new(ScalarValue::Int8(Some(1)));
                let op = BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs));
                Count::<Gt>::try_new(&right_plan.schema(), Arc::new(op), 0)?
            };

            Segment::new(Some(Arc::new(left_expr)), Some(Arc::new(right_expr)))
        };

        let s4 = {
            let left_expr = {
                let lhs = Column::new_with_schema("b1", &left_plan.schema())?;
                let rhs = Literal::new(ScalarValue::Int8(Some(1)));
                BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs))
            };

            let right_expr = {
                let lhs = Column::new_with_schema("b2", &right_plan.schema())?;
                let rhs = Literal::new(ScalarValue::Int8(Some(1)));
                let op = BinaryExpr::new(Arc::new(lhs), Operator::Eq, Arc::new(rhs));
                Count::<Gt>::try_new(&right_plan.schema(), Arc::new(op), 0)?
            };

            Segment::new(Some(Arc::new(left_expr)), Some(Arc::new(right_expr)))
        };

        let schema = Arc::new(Schema::new(vec![
            Field::new("segment", DataType::Int8, false),
            Field::new("u1", DataType::Int8, false),
            Field::new("a1", DataType::Int8, false),
            Field::new("b1", DataType::Int8, false),
            Field::new("a2", DataType::Int8, false),
            Field::new("b2", DataType::Int8, false),
        ]));

        let join = JoinExec::try_new(
            left_plan.clone(),
            right_plan.clone(),
            (
                Column::new_with_schema("u1", &left_plan.schema())?,
                Column::new_with_schema("u2", &right_plan.schema())?,
            ),
            vec![s1, s2, s3, s4],
            schema.clone(),
            2,
        )?;

        let stream = join.execute(0).await?;
        let batches = common::collect(stream).await?;

        let exp_batches = build_batches(schema.clone(), &exp());

        assert_eq!(exp_batches.len(), batches.len());

        for (id, batch) in batches.iter().enumerate() {
            assert_eq!(
                arrow::util::pretty::pretty_format_batches(&[exp_batches[id].clone()])?,
                arrow::util::pretty::pretty_format_batches(&[batch.clone()])?,
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_plan() -> Result<()> {
        let mut ctx = make_context();

        let left_plan = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("u1", DataType::Int8, false),
                Field::new("a1", DataType::Int8, false),
                Field::new("b1", DataType::Int8, false),
            ]));
            let batches = build_batches(schema.clone(), &users());
            Arc::new(LogicalPlanBuilder::scan_memory(vec![batches], schema.clone(), None)?.build()?)
        };

        let right_plan = {
            let schema = Arc::new(Schema::new(vec![
                Field::new("u2", DataType::Int8, false),
                Field::new("a2", DataType::Int8, false),
                Field::new("b2", DataType::Int8, false),
            ]));
            let batches = build_batches(schema.clone(), &events());
            Arc::new(LogicalPlanBuilder::scan_memory(vec![batches], schema.clone(), None)?.build()?)
        };

        let schema = Schema::new(vec![
            Field::new("segment", DataType::Int8, false),
            Field::new("u1", DataType::Int8, false),
            Field::new("a1", DataType::Int8, false),
            Field::new("b1", DataType::Int8, false),
            Field::new("a2", DataType::Int8, false),
            Field::new("b2", DataType::Int8, false),
        ]);

        let s1 = {
            let left_expr = {
                LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "a1",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                }
            };

            LogicalSegment::new(Some(left_expr), None)
        };

        let s2 = {
            let right_expr = {
                let p = LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "a2",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                };

                SegmentExpr::Count {
                    predicate: Box::new(p),
                    op: Operator::Gt,
                    right: 0,
                }
            };

            LogicalSegment::new(None, Some(right_expr))
        };

        let s3 = {
            let left_expr = {
                LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "a1",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                }
            };

            let right_expr = {
                let p = LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "a2",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                };

                SegmentExpr::Count {
                    predicate: Box::new(p),
                    op: Operator::Gt,
                    right: 0,
                }
            };

            LogicalSegment::new(Some(left_expr), Some(right_expr))
        };

        let s4 = {
            let left_expr = {
                LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "b1",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                }
            };

            let right_expr = {
                let p = LogicalExpr::BinaryExpr {
                    left: Box::new(LogicalExpr::Column(LogicalColumn::from_qualified_name(
                        "b2",
                    ))),
                    op: Operator::Eq,
                    right: Box::new(LogicalExpr::Literal(ScalarValue::Int8(Some(1)))),
                };

                SegmentExpr::Count {
                    predicate: Box::new(p),
                    op: Operator::Gt,
                    right: 0,
                }
            };

            LogicalSegment::new(Some(left_expr), Some(right_expr))
        };

        let join_node = JoinPlanNode::try_new(
            left_plan.clone(),
            right_plan.clone(),
            (
                LogicalColumn::from_qualified_name("u1"),
                LogicalColumn::from_qualified_name("u2"),
            ),
            vec![s1, s2, s3, s4],
            Arc::new(DFSchema::try_from(schema.clone())?),
            2,
        )?;

        let join = LogicalPlan::Extension {
            node: Arc::new(join_node.clone()),
        };

        println!("{:?}", join_node.clone());
        Ok(())
    }
}
