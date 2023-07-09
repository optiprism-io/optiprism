use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Index;
use std::ops::IndexMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Int64Array;
use arrow::compute::concat_batches;
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::Statistics;
use datafusion_expr::ColumnarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::expressions::partitioned::SegmentationExpr;

pub struct SegmentationExec {
    segments: Vec<Arc<dyn SegmentationExpr>>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    columns: Vec<Column>,
    out_batch_size: usize,
}

impl Debug for SegmentationExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentationExec")
    }
}

impl SegmentationExec {
    pub fn try_new(
        segments: Vec<Arc<dyn SegmentationExpr>>,
        columns: Vec<Column>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let mut schema = input.schema().project(
            columns
                .iter()
                .map(|c| c.index())
                .collect::<Vec<_>>()
                .as_slice(),
        )?;
        schema.fields = vec![
            vec![Field::new("segment", DataType::Int8, false)],
            schema.fields,
        ]
        .concat();

        let res = Self {
            segments,
            input,
            schema: Arc::new(schema),
            metrics: Default::default(),
            columns,
            out_batch_size,
        };

        Ok(res)
    }
}

#[async_trait]
impl ExecutionPlan for SegmentationExec {
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
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentationExec::try_new(
                self.segments.clone(),
                self.columns.clone(),
                children[0].clone(),
                self.out_batch_size,
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(SegmentationStream {
            segments: self.segments.clone(),
            columns: self.columns.clone(),
            input: self.input.execute(partition, context.clone())?,
            schema: self.schema.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            is_ended: false,
            out_batch_size: self.out_batch_size,
            random_state: ahash::RandomState::with_seeds(0, 0, 0, 0),
            batch_buf: FixedVecDeque::new(2),
            spans_buf: vec![FixedVecDeque::new(2); self.segments.len()],
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentationExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

pub struct SegmentationStream {
    segments: Vec<Arc<dyn SegmentationExpr>>,
    columns: Vec<Column>,
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    is_ended: bool,
    out_batch_size: usize,
    random_state: ahash::RandomState,
    batch_buf: FixedVecDeque<RecordBatch>,
    spans_buf: Vec<FixedVecDeque<Vec<i64>>>,
}

fn take_from_batch(
    batch: RecordBatch,
    to_take: Vec<i64>,
    cols: &Vec<Column>,
    schema: SchemaRef,
    segment_id: i8,
) -> Result<RecordBatch> {
    let to_take = Int64Array::from(to_take);
    let batch = batch.project(
        cols.iter()
            .map(|c| c.index())
            .collect::<Vec<_>>()
            .as_slice(),
    )?;
    let res_arrs = batch
        .columns()
        .iter()
        .map(|c| take(c, &to_take, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let segment_col = ScalarValue::from(segment_id).to_array_of_size(to_take.len());

    let out = RecordBatch::try_new(
        schema,
        vec![vec![segment_col], res_arrs.into_iter().collect::<Vec<_>>()].concat(),
    )?;

    Ok(out)
}

#[derive(Clone, Debug)]
struct FixedVecDeque<T> {
    inner: VecDeque<T>,
    capacity: usize,
}

impl<T> FixedVecDeque<T> {
    fn new(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push_back(&mut self, item: T) {
        if self.inner.len() == self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(item);
    }

    fn back(&self) -> Option<&T> {
        self.inner.back()
    }

    fn pop_front(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    fn pop_back(&mut self) -> Option<T> {
        self.inner.pop_back()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T> Index<usize> for FixedVecDeque<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<T> IndexMut<usize> for FixedVecDeque<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

impl Stream for SegmentationStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let mut hash_buf = vec![];
        let mut res_buf: Vec<FixedVecDeque<Option<BooleanArray>>> =
            vec![FixedVecDeque::new(2); self.segments.len()];
        let mut out_buf: VecDeque<RecordBatch> = VecDeque::with_capacity(100);
        let mut spans_buf = self.spans_buf.clone();
        let mut last_hash = 0;
        loop {
            let mut spans = vec![];
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.batch_buf.push_back(batch.clone());
                    let num_rows = batch.num_rows();
                    hash_buf.resize(num_rows, 0);
                    let pk_arrs = self
                        .columns
                        .iter()
                        .map(|v| v.evaluate(&batch))
                        .collect::<std::result::Result<Vec<ColumnarValue>, _>>()?
                        .iter()
                        .map(|v| v.clone().into_array(batch.num_rows()).clone())
                        .collect::<Vec<ArrayRef>>();

                    create_hashes(&pk_arrs, &mut self.random_state, &mut hash_buf).unwrap();

                    // todo remove
                    let hash_buf = batch.columns()[0]
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap() as u64)
                        .collect::<Vec<_>>();
                    for (idx, segment) in self.segments.iter().enumerate() {
                        let res = segment
                            .evaluate(&batch, &hash_buf)
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        let res =
                            res.map(|v| v.as_any().downcast_ref::<BooleanArray>().unwrap().clone());

                        // println!("{:?}", self.batch_buf);
                        // println!("{:?}", res);
                        res_buf[idx].push_back(res.clone());
                    }
                    for (idx, p) in hash_buf.iter().enumerate() {
                        if last_hash == 0 {
                            last_hash = *p;
                        }
                        if last_hash != *p {
                            spans.push(idx as i64);
                            last_hash = *p;
                        }
                    }

                    for (idx, _) in self.segments.iter().enumerate() {
                        spans_buf[idx].push_back(spans.clone());
                    }
                }
                Poll::Ready(None) => {
                    for (idx, segment) in self.segments.iter().enumerate() {
                        let res = segment
                            .finalize()
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap().clone();

                        res_buf[idx].push_back(Some(res.clone()));
                    }

                    let n = self.batch_buf[1].num_rows();
                    for (idx, _) in self.segments.iter().enumerate() {
                        spans_buf[idx][0].push(n as i64);
                    }

                    self.is_ended = true;
                }
                other => return other,
            };

            for (idx, _) in self.segments.iter().enumerate() {
                if res_buf[idx].back().unwrap().is_some() {
                    let res = res_buf[idx].pop_back().unwrap();
                    let spans = spans_buf[idx].pop_back().unwrap();
                    // println!("{idx} {:?}", spans);

                    let mut filtered_spans = spans
                        .into_iter()
                        .zip(res.unwrap().iter())
                        .filter_map(|(v, b)| match b.unwrap() {
                            true => Some(v),
                            false => None,
                        })
                        .collect::<VecDeque<_>>();
                    let cur_batch = self.batch_buf[1].clone();
                    if filtered_spans.len() > 0 {
                        if filtered_spans[0] == 0 && self.batch_buf.len() > 0 {
                            let prev_batch = self.batch_buf[0].clone();
                            filtered_spans.pop_front();
                            let row_id = prev_batch.num_rows() as i64 - 1;
                            let out = take_from_batch(
                                prev_batch,
                                vec![row_id],
                                &self.columns,
                                self.schema.clone(),
                                idx as i8,
                            )
                            .map_err(|err| err.into_datafusion_execution_error())?;
                            out_buf.push_back(out);
                        } else {
                            let out = take_from_batch(
                                cur_batch,
                                filtered_spans
                                    .into_iter()
                                    .map(|v| v - 1)
                                    .collect::<Vec<i64>>(),
                                &self.columns,
                                self.schema.clone(),
                                idx as i8,
                            )
                            .map_err(|err| err.into_datafusion_execution_error())?;
                            out_buf.push_back(out);
                        }
                    }
                }
            }

            let rows = out_buf.iter().map(|b| b.num_rows()).sum::<usize>();
            if rows > self.out_batch_size || self.is_ended {
                self.spans_buf = spans_buf;
                let rb = concat_batches(
                    &self.schema(),
                    out_buf.iter().map(|v| v).collect::<Vec<_>>(),
                )?;
                return self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(rb))));
            }
        }
    }
}

impl RecordBatchStream for SegmentationStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Float32Array;
    use arrow::array::Int16Array;
    use arrow::array::Int64Array;
    use arrow::array::Int64Builder;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use futures::StreamExt;
    use partitioned::SegmentationExpr;
    use store::test_util::parse_markdown_table_v1;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned;
    use crate::physical_plan::expressions::partitioned::boolean_op;
    use crate::physical_plan::expressions::partitioned::count::Count;
    // use crate::physical_plan::expressions::segmentation::comparison;
    // use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::segmentation::SegmentationExec;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 1            | 1      | 1           |
|              |        |             |
| 2            | 2      | 1           |
|              |        |             |
| 3            | 3      | 1           |
| 3            | 4      | 1           |
| 3            | 5      | 1           |
|              |        |             |
| 3            | 6      | 1           |
| 3            | 7      | 1           |
| 3            | 8      | 1           |
|              |        |             |
| 3            | 9      | 1           |
| 3            | 10     | 1           |
| 3            | 11     | 1           |
|              |        |             |
| 3            | 12     | 1           |
| 4            | 13     | 1           |
| 5            | 14     | 1           |
| 6            | 15     | 1           |
| 6            | 16     | 1           |
| 6            | 17     | 1           |
| 7            | 18     | 1           |
| 7            | 19     | 1           |
| 8            | 20     | 1           |
|              |        |             |
| 9            | 21     | 1           |
| 9            | 22     | 1           |
| 10           | 23     | 1           |
| 11           | 24     | 1           |
|              |        |             |
| 12           | 25     | 1           |
| 12           | 26     | 1           |
| 12           | 27     | 1           |
| 12           | 28     | 1           |
|              |        |             |
| 12           | 29     | 1           |
| 12           | 30     | 1           |
| 12           | 31     | 1           |
| 12           | 32     | 1           |
|              |        |             |
| 12           | 33     | 1           |
| 12           | 34     | 1           |
|              |        |             |
| 12           | 35     | 1           |
| 13           | 36     | 1           |
| 14           | 37     | 1           |
| 15           | 38     | 1           |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;
        let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
        let f = Arc::new(BinaryExpr::new(left, Operator::GtEq, right));
        let mut agg1 = Arc::new(Count::<boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("ts", &schema).unwrap(),
            1,
            TimeRange::None,
            None,
        ));
        let mut agg2 = Arc::new(Count::<boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("ts", &schema).unwrap(),
            2,
            TimeRange::None,
            None,
        ));
        let mut agg3 = Arc::new(Count::<boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("ts", &schema).unwrap(),
            3,
            TimeRange::None,
            None,
        ));

        let segmentation = SegmentationExec::try_new(
            vec![agg1 /* , agg2, agg3 */],
            vec![Column::new_with_schema("user_id", &schema)?],
            Arc::new(input),
            0,
        )?;

        fn batch_to_arr(v: &ArrayRef) -> Vec<i64> {
            let v = v.as_any().downcast_ref::<Int64Array>().unwrap();
            v.iter().map(|x| x.unwrap()).collect()
        }

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = segmentation.execute(0, task_ctx)?;
        let result = collect(stream).await?;
        // for i in result.iter() {
        // println!("{:?}", batch_to_arr(&i.columns()[1]));
        // }
        let exp = Int64Array::from(vec![1, 2, 3, 4, 7, 10, 12, 14, 15]);
        let exp = Arc::new(exp) as ArrayRef;
        print_batches(&result).unwrap();
        let res: ArrayRef = result[0].columns()[1].clone();
        assert_eq!(&*res, &*exp);
        Ok(())
    }
}
