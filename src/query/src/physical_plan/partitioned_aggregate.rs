use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::ArrayRef;
use arrow::array::UInt32Array;
use arrow::compute::concat_batches;
use arrow::compute::take;
use arrow::datatypes::Field;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::TaskContext;
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
use datafusion_common::Statistics;
use datafusion_expr::ColumnarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::PartitionState;

pub trait PartitionedAggregateExpr: Send + Sync {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<ArrayRef>>;
    fn fields(&self) -> Vec<Field>;
}

pub struct PartitionedAggregateExec {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    out_batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionedAggregateExec {
    pub fn try_new(
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let ret = Self {
            group_expr,
            agg_expr,
            schema: input.schema(),
            input,
            out_batch_size,
            metrics: Default::default(),
        };

        Ok(ret)
    }
}

#[async_trait]
impl ExecutionPlan for PartitionedAggregateExec {
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
            PartitionedAggregateExec::try_new(
                self.group_expr.clone(),
                self.agg_expr.clone(),
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
        Ok(Box::pin(PartitionedAggregateStream {
            group_expr: self.group_expr.clone(),
            agg_expr: self.agg_expr.clone(),
            schema: self.schema.clone(),
            state: PartitionState::new(self.group_expr.clone()),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            is_ended: false,
            out_batch_size: self.out_batch_size.clone(),
            input: self.input.execute(partition, context.clone())?,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PartitionedAggregateExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl Debug for PartitionedAggregateExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionedAggregateExec")
    }
}

pub struct PartitionedAggregateStream {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
    schema: SchemaRef,
    state: PartitionState,
    baseline_metrics: BaselineMetrics,
    is_ended: bool,
    out_batch_size: usize,
    input: SendableRecordBatchStream,
}

impl Stream for PartitionedAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let mut out_buf: VecDeque<RecordBatch> = Default::default();
        let mut out_res_buf: VecDeque<RecordBatch> = Default::default();
        let mut to_take: HashMap<usize, Vec<usize>> = Default::default();

        loop {
            let mut offset = 0;
            // let timer = self.baseline_metrics.elapsed_compute().timer();
            let res = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // push batch to state
                    if let Some((batches, spans, skip)) = self.state.push(batch)? {
                        // state gives us a batch to process
                        offset = skip;
                        // evaluate
                        let ev_res = self
                            .agg_expr
                            .iter()
                            .map(|expr| expr.evaluate(&batches, spans.clone(), skip))
                            .collect::<Result<Vec<Vec<ArrayRef>>>>()
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        Some((batches, spans, ev_res))
                    } else {
                        None
                    }
                }
                Poll::Ready(None) => {
                    // no more batches, finalize the funnel
                    self.is_ended = true;
                    if let Some((batches, spans, skip)) = self.state.finalize()? {
                        offset = skip;
                        let ev_res = self
                            .agg_expr
                            .iter()
                            .map(|expr| expr.evaluate(&batches, spans.clone(), skip))
                            .collect::<Result<Vec<Vec<ArrayRef>>>>()
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        Some((batches, spans, ev_res))
                    } else {
                        None
                    }
                }
                other => return other,
            };

            if let Some((batches, spans, ev_res)) = res {
                for span in spans.into_iter() {
                    let (batch_id, row_id) = abs_row_id(offset, &batches);
                    // make arrays of indexes to take
                    to_take.entry(batch_id).or_default().push(row_id);
                    offset += span;
                }
                let mut batches = mem::replace(&mut to_take, Default::default())
                    .iter()
                    .map(|(batch_id, rows)| {
                        // make take array from usize
                        let take_arr = rows.iter().map(|b| Some(*b as u32)).collect::<Vec<_>>();
                        let take_arr = UInt32Array::from(take_arr);
                        // actual take of arrs
                        let cols = batches[*batch_id]
                            .columns()
                            .iter()
                            .map(|arr| take(arr, &take_arr, None))
                            .collect::<std::result::Result<Vec<_>, _>>()?;

                        // println!("0 {:?}", cols);
                        // println!("1 {:?}", ev_res[0]);
                        let ev_arrs = ev_res.iter().flat_map(|v| v.to_owned()).collect::<Vec<_>>();
                        // final columns
                        // let arrs = vec![ev_arrs, cols].concat();
                        let arrs = cols;

                        // final record batch
                        RecordBatch::try_new(self.schema.clone(), arrs)
                    })
                    .collect::<Vec<std::result::Result<_, _>>>()
                    .into_iter()
                    .collect::<std::result::Result<Vec<RecordBatch>, _>>()?;

                let res_batches = ev_res[0].iter().map(|arr| {}).collect::<Vec<_>>();
                println!("0 {:?}", batches);
                println!("1 {:?}", ev_res[0]);
                // push to buffer
                out_buf.append(&mut batches.into());
            }

            let rows = out_buf.iter().map(|b| b.num_rows()).sum::<usize>();
            // return buffer only if it's full or we are at the end
            if rows > self.out_batch_size || self.is_ended {
                let rb =
                    concat_batches(&self.schema, out_buf.iter().map(|v| v).collect::<Vec<_>>())?;
                return self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(rb))));
            }

            // timer.done();
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for PartitionedAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::count_span::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExec;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
    use crate::physical_plan::PartitionState;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0       | 1   | e1    | 1      |
| 0       | 2   | e2    | 1      |
| 0       | 3   | e3    | 1      |
| 0       | 4   | e1    | 1      |
|         |     |       |        |
| 0       | 5   | e1    | 1      |
| 0       | 6   | e2    | 1      |
| 0       | 7   | e3    | 1      |
| 1       | 5   | e1    | 1      |
| 1       | 6   | e3    | 1      |
| 1       | 7   | e1    | 1      |
| 1       | 8   | e2    | 1      |
| 2       | 9   | e1    | 1      |
| 2       | 10  | e2    | 1      |
|         |     |       |        |
| 2       | 11  | e3    | 1      |
| 2       | 12  | e3    | 1      |
| 2       | 13  | e3    | 1      |
| 2       | 14  | e3    | 1      |
| 3       | 15  | e1    | 1      |
| 3       | 16  | e2    | 1      |
| 3       | 17  | e3    | 1      |
| 4       | 18  | e1    | 1      |
| 4       | 19  | e2    | 1      |
|         |     |       |        |
| 4       | 20  | e3    | 1      |
|         |     |       |        |
| 5       | 21  | e1    | 1      |
|         |     |       |        |
| 5       | 22  | e2    | 1      |
|         |     |       |        |
| 6       | 23  | e1    | 1      |
| 6       | 24  | e3    | 1      |
| 6       | 25  | e3    | 1      |
|         |     |       |        |
| 7       | 26  | e1    | 1      |
| 7       | 27  | e2    | 1      |
| 7       | 28  | e3    | 1      |
|         |     |       |        |
| 8       | 29  | e1    | 1      |
| 8       | 30  | e2    | 1      |
| 8       | 31  | e3    | 1      |
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
            2,
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
        let segmentation = PartitionedAggregateExec::try_new(
            vec![Arc::new(Column::new_with_schema("user_id", &schema)?)],
            vec![agg1],
            Arc::new(input),
            1,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut stream = segmentation.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn it_works2() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1           |
| 0            | 2      | 1           |
| 0            | 3      | 1           |
| 0            | 4      | 1           |
|              |        |             |
| 0            | 5      | 1           |
| 0            | 6      | 1           |
| 0            | 7      | 1           |
| 1            | 5      | 1           |
| 1            | 6      | 1           |
| 1            | 7      | 1           |
| 1            | 8      | 1           |
| 2            | 9      | 1           |
| 2            | 10     | 1           |
|              |        |             |
| 2            | 11     | 1           |
| 2            | 12     | 1           |
| 2            | 13     | 1           |
| 2            | 14     | 1           |
| 3            | 15     | 1           |
| 3            | 16     | 1           |
| 3            | 17     | 1           |
| 4            | 18     | 1           |
| 4            | 19     | 1           |
|              |        |             |
| 4            | 20     | 1           |
|              |        |             |
| 5            | 21     | 1           |
|              |        |             |
| 5            | 22     | 1           |
|              |        |             |
| 6            | 23     | 1           |
| 6            | 24     | 1           |
| 6            | 25     | 1           |
|              |        |             |
| 7            | 26     | 1           |
| 7            | 27     | 1           |
| 7            | 28     | 1           |
|              |        |             |
| 8            | 29     | 1           |
| 8            | 30     | 1           |
| 8            | 31     | 1           |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let col = Arc::new(Column::new_with_schema("user_id", &schema).unwrap());
        let mut state = PartitionState::new(vec![col]);

        let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<boolean_op::Eq>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &schema).unwrap(),
            3,
            TimeRange::None,
            None,
        );

        for batch in batches {
            if let Some((batches, spans, skip)) = state.push(batch).unwrap() {
                println!("spans: {:?} {}", spans, skip);
                // println!("batches: {:?}", batches);
                let res = count.evaluate(&batches, spans, skip).unwrap();
                // println!("res: {:?}", res);
            }
        }

        if let Some((batches, spans, skip)) = state.finalize().unwrap() {
            println!("spans: {:?} {}", spans, skip);
            let res = count.evaluate(&batches, spans, skip).unwrap();
            println!("final: {:?}", res);
        }

        Ok(())
    }
}
