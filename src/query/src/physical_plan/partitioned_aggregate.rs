use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::UInt32Array;
use arrow::compute::concat_batches;
use arrow::compute::take;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
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

pub trait SegmentExpr: Send + Sync {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<BooleanArray>;
}

pub struct SegmentExprWrap {
    expr: Box<dyn PartitionedAggregateExpr>,
}

impl SegmentExpr for SegmentExprWrap {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<BooleanArray> {
        let resp = self.expr.evaluate(batches, spans, skip)?;
        Ok(resp[0]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone())
    }
}

impl SegmentExprWrap {
    pub fn new(expr: Box<dyn PartitionedAggregateExpr>) -> Self {
        Self { expr }
    }
}

pub struct PartitionedAggregateExec {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
    schema: SchemaRef,
    agg_schema: SchemaRef,
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
        let agg_schemas = agg_expr
            .iter()
            .map(|a| (*a.schema()).clone())
            .collect::<Vec<_>>();
        let agg_schema = Schema::try_merge(agg_schemas)?;
        let input_schema = (*input.schema()).clone();
        let schema =
            Schema::try_merge(vec![vec![agg_schema.clone()], vec![input_schema]].concat())?;
        let ret = Self {
            group_expr,
            agg_expr,
            schema: Arc::new(schema),
            agg_schema: Arc::new(agg_schema),
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
            agg_schema: self.agg_schema.clone(),
            input_schema: self.input.schema(),
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
    agg_schema: SchemaRef,
    input_schema: SchemaRef,
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
        let mut to_take: BTreeMap<usize, Vec<usize>> = Default::default();

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

                        // final record batch
                        RecordBatch::try_new(self.input_schema.clone(), cols)
                    })
                    .collect::<Vec<std::result::Result<_, _>>>()
                    .into_iter()
                    .collect::<std::result::Result<Vec<RecordBatch>, _>>()?;

                let ev_cols = ev_res.iter().flat_map(|v| v.clone()).collect::<Vec<_>>();
                let res_batch = RecordBatch::try_new(self.agg_schema.clone(), ev_cols)?;
                out_res_buf.push_back(res_batch);

                // push to buffer
                out_buf.append(&mut batches.into());
            }

            let rows = out_buf.iter().map(|b| b.num_rows()).sum::<usize>();
            // return buffer only if it's full or we are at the end
            if rows >= self.out_batch_size || self.is_ended {
                let rb = concat_batches(
                    &self.input_schema,
                    out_buf.iter().map(|v| v).collect::<Vec<_>>(),
                )?;

                let evb = concat_batches(
                    &self.agg_schema,
                    out_res_buf.iter().map(|v| v).collect::<Vec<_>>(),
                )?;

                let final_batch = RecordBatch::try_new(
                    self.schema.clone(),
                    vec![evb.columns(), rb.columns()].concat(),
                )?;

                return self
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(Ok(final_batch))));
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

    use arrow::array::ArrayRef;
    use arrow::array::UInt32Array;
    use arrow::compute::take;
    use arrow::util::pretty::print_batches;
    use chrono::Duration;
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

    use crate::event_eq;
    use crate::physical_plan::expressions::partitioned::funnel::funnel;
    use crate::physical_plan::expressions::partitioned::funnel::funnel::FunnelExpr;
    use crate::physical_plan::expressions::partitioned::funnel::funnel::FunnelExprWrap;
    use crate::physical_plan::expressions::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned::funnel::Touch;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExec;
    use crate::physical_plan::PartitionState;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0       | 1   | e1    |
| 0       | 2   | e2    |
| 0       | 3   | e3    |
| 0       | 4   | e1    |
|         |     |       |
| 0       | 5   | e1    |
| 0       | 6   | e2    |
| 0       | 7   | e3    |
| 0       | 8   | e1    |
|         |     |       |
| 0       | 9   | e1    |
| 0       | 10   | e2    |
| 0       | 11   | e3    |
| 0       | 12   | e1    |
|         |     |       |
| 0       | 13   | e1    |
| 0       | 14   | e2    |
| 0       | 15   | e3    |
| 1       | 5   | e1    |
| 1       | 6   | e3    |
| 1       | 7   | e1    |
| 1       | 8   | e2    |
| 2       | 9   | e1    |
| 2       | 10  | e2    |
|         |     |       |
| 2       | 11  | e3    |
| 2       | 12  | e3    |
| 2       | 13  | e3    |
| 2       | 14  | e3    |
| 3       | 15  | e1    |
| 3       | 16  | e2    |
| 3       | 17  | e3    |
| 4       | 18  | e1    |
| 4       | 19  | e2    |
|         |     |       |
| 4       | 20  | e3    |
|         |     |       |
| 4       | 20  | e3    |
|         |     |       |
| 4       | 20  | e3    |
|         |     |       |
| 5       | 21  | e1    |
|         |     |       |
| 5       | 22  | e2    |
|         |     |       |
| 6       | 23  | e1    |
| 6       | 24  | e3    |
| 6       | 25  | e3    |
|         |     |       |
| 7       | 26  | e1    |
| 7       | 27  | e2    |
| 7       | 28  | e3    |
|         |     |       |
| 8       | 29  | e1    |
| 8       | 30  | e2    |
| 8       | 31  | e3    |
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
            5,
            TimeRange::None,
            None,
            "a".to_string(),
        ));

        let mut agg2 = Arc::new(Count::<boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("ts", &schema).unwrap(),
            2,
            TimeRange::None,
            None,
            "b".to_string(),
        ));

        let e1 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e2".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e3".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let opts = funnel::Options {
            ts_col: Column::new_with_schema("ts", &schema)?,
            window: Duration::seconds(15),
            steps: vec![e1, e2, e3],
            exclude: None,
            constants: None,
            count: Unique,
            filter: None,
            touch: Touch::First,
        };

        let f = FunnelExpr::new(opts);
        let agg3 = Arc::new(FunnelExprWrap::new(f));
        let segmentation = PartitionedAggregateExec::try_new(
            vec![Arc::new(Column::new_with_schema("user_id", &schema)?)],
            vec![agg1, agg2, agg3],
            Arc::new(input),
            100,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = segmentation.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn group_by_day() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | e1          |
| 0            | 1      | e2          |
| 0            | 1      | e3          |
| 0            | 2      | e1          |
| 0            | 2      | e2          |
| 0            | 2      | e3          |
| 0            | 3      | e1          |
| 0            | 3      | e2          |
| 0            | 3      | e3          |
| 1            | 1      | e2          |
| 1            | 1      | e2          |
| 1            | 1      | e3          |
| 1            | 2      | e1          |
| 1            | 2      | e2          |
| 1            | 2      | e3          |
| 1            | 3      | e1          |
| 1            | 3      | e2          |
| 1            | 3      | e3          |
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
            5,
            TimeRange::None,
            None,
            "a".to_string(),
        ));

        let mut agg2 = Arc::new(Count::<boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("ts", &schema).unwrap(),
            2,
            TimeRange::None,
            None,
            "b".to_string(),
        ));

        let e1 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e2".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e3".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let opts = funnel::Options {
            ts_col: Column::new_with_schema("ts", &schema)?,
            window: Duration::seconds(15),
            steps: vec![e1, e2, e3],
            exclude: None,
            constants: None,
            count: Unique,
            filter: None,
            touch: Touch::First,
        };

        let f = FunnelExpr::new(opts);
        let agg3 = Arc::new(FunnelExprWrap::new(f));
        let segmentation = PartitionedAggregateExec::try_new(
            vec![
                Arc::new(Column::new_with_schema("user_id", &schema)?),
                Arc::new(Column::new_with_schema("ts", &schema)?),
            ],
            vec![agg1, agg2, agg3],
            Arc::new(input),
            100,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = segmentation.execute(0, task_ctx)?;
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
            4,
            TimeRange::None,
            None,
            "b".to_string(),
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
