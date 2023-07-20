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
use arrow::array::ArrayAccessor;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::Int32Array;
use arrow::array::UInt32Array;
use arrow::array::UInt32Builder;
use arrow::compute::and;
use arrow::compute::concat_batches;
use arrow::compute::or;
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
use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
use crate::physical_plan::partitioned_aggregate::SegmentExpr;
use crate::physical_plan::PartitionState;

pub struct SegmentedAggregateExpr {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    segment_expr: Vec<Arc<dyn SegmentExpr>>,
    agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
    input: Arc<dyn ExecutionPlan>,
    agg_schema: SchemaRef,
    schema: SchemaRef,
    out_batch_size: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl SegmentedAggregateExpr {
    pub fn try_new(
        group_expr: Vec<Arc<dyn PhysicalExpr>>,
        segment_expr: Vec<Arc<dyn SegmentExpr>>,
        agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let seg_schema = Schema::new(vec![Field::new(
            "segment",
            arrow::datatypes::DataType::UInt32,
            false,
        )]);
        let input_schema = (*input.schema()).clone();

        let agg_schemas = agg_expr
            .iter()
            .map(|a| (*a.schema()).clone())
            .collect::<Vec<_>>();
        let agg_schema = Schema::try_merge(agg_schemas)?;
        let schema = Schema::try_merge(
            vec![vec![seg_schema, agg_schema.clone()], vec![input_schema]].concat(),
        )?;

        let ret = Self {
            group_expr,
            segment_expr,
            agg_expr,
            input,
            agg_schema: Arc::new(agg_schema),
            schema: Arc::new(schema),
            out_batch_size,
            metrics: Default::default(),
        };

        Ok(ret)
    }
}

#[async_trait]
impl ExecutionPlan for SegmentedAggregateExpr {
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
            SegmentedAggregateExpr::try_new(
                self.group_expr.clone(),
                self.segment_expr.clone(),
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
        Ok(Box::pin(SegmentedAggregateStream {
            group_expr: self.group_expr.clone(),
            segment_expr: self.segment_expr.clone(),
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
        write!(f, "SegmentedAggregateExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl Debug for SegmentedAggregateExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentedAggregateExec")
    }
}

pub struct SegmentedAggregateStream {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    segment_expr: Vec<Arc<dyn SegmentExpr>>,
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

impl Stream for SegmentedAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let mut seg_out: UInt32Builder = UInt32Builder::new();
        let mut out_buf: VecDeque<RecordBatch> = Default::default();
        let mut out_res_buf: VecDeque<RecordBatch> = Default::default();
        let mut to_take: BTreeMap<usize, Vec<usize>> = Default::default();
        let mut to_take_ev: Vec<i32> = Default::default();

        loop {
            let mut offset = 0;
            // let timer = self.baseline_metrics.elapsed_compute().timer();
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let maybe_res = self.state.push(batch)?;
                    if maybe_res.is_none() {
                        continue;
                    }

                    let (batches, spans, skip) = maybe_res.unwrap();
                    offset = skip;

                    // evaluate agg
                    let ev_res = self
                        .agg_expr
                        .iter()
                        .map(|expr| expr.evaluate(&batches, spans.clone(), skip))
                        .collect::<Result<Vec<_>>>()
                        .map_err(|e| e.into_datafusion_execution_error())?;

                    // evaluate segments
                    let seg_res = self
                        .segment_expr
                        .iter()
                        .map(|expr| expr.evaluate(&batches, spans.clone(), skip))
                        .collect::<Result<Vec<_>>>()
                        .map_err(|e| e.into_datafusion_execution_error())?;

                    for (span_idx, span) in spans.iter().enumerate() {
                        let (batch_id, row_id) = abs_row_id(offset, &batches);
                        offset += span;
                        for (seg_idx, valid) in seg_res.iter().enumerate() {
                            if !valid.value(span_idx) {
                                continue;
                            }

                            seg_out.append_value(seg_idx as u32);
                            to_take_ev.push(span_idx as i32);
                            to_take.entry(batch_id).or_default().push(row_id);
                        }

                        // evaluate agg
                    }
                    if seg_out.len() == 0 {
                        println!("!");
                        // todo return empty batch
                        continue;
                    }

                    println!("!в");
                    let to_take_idx =
                        Int32Array::from(mem::replace(&mut to_take_ev, Default::default()));
                    println!("{:?}\n", to_take_idx);
                    println!("{:?}\n", ev_res);

                    let ev_res_arrs = ev_res
                        .iter()
                        .flat_map(|arr| {
                            arr.iter()
                                .map(|arr| take(arr, &to_take_idx, None).unwrap())
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();
                    println!("?");
                    let mut group_batches = mem::replace(&mut to_take, Default::default())
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

                    // push to buffer
                    out_buf.append(&mut group_batches.into());
                    let ev_res_batch = RecordBatch::try_new(self.agg_schema.clone(), ev_res_arrs)?;
                    out_res_buf.push_back(ev_res_batch);

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

                        let sc = Arc::new(seg_out.finish()) as ArrayRef;
                        let final_batch = RecordBatch::try_new(
                            self.schema.clone(),
                            vec![&vec![sc], evb.columns(), rb.columns()].concat(),
                        )?;

                        return self
                            .baseline_metrics
                            .record_poll(Poll::Ready(Some(Ok(final_batch))));
                    }
                }
                other => return other,
            }

            // timer.done();
        }
        Poll::Ready(None)
    }
}

impl RecordBatchStream for SegmentedAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
    use crate::physical_plan::expressions::partitioned::aggregate;
    use crate::physical_plan::expressions::partitioned::boolean_op;
    use crate::physical_plan::expressions::partitioned::cond_aggregate::Aggregate;
    use crate::physical_plan::expressions::partitioned::cond_count::Count;
    use crate::physical_plan::expressions::partitioned::funnel::funnel;
    use crate::physical_plan::expressions::partitioned::funnel::funnel::FunnelExpr;
    use crate::physical_plan::expressions::partitioned::funnel::funnel::FunnelExprWrap;
    use crate::physical_plan::expressions::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned::funnel::Touch;
    use crate::physical_plan::expressions::partitioned::time_range::TimeRange;
    use crate::physical_plan::expressions::partitioned::AggregateFunction;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExec;
    use crate::physical_plan::partitioned_aggregate::PartitionedAggregateExpr;
    use crate::physical_plan::partitioned_aggregate::SegmentExprWrap;
    use crate::physical_plan::segmented_aggregate::SegmentedAggregateExpr;
    use crate::physical_plan::PartitionState;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|-------|--------|-------------|
| 0            | iphone       | 1     | 1      | e1          |
| 0            | iphone       | 0     | 2      | e2          |
| 0            | iphone       | 0     | 3      | e3          |
| 0            | android      | 1     | 4      | e1          |
| 0            | android      | 1     | 5      | e2          |
| 0            | android      | 0     | 6      | e3          |
| 1            | osx          | 1     | 1      | e1          |
| 1            | osx          | 1     | 2      | e2          |
| 1            | osx          | 0     | 3      | e3          |
| 1            | osx          | 0     | 4      | e1          |
| 1            | osx          | 0     | 5      | e2          |
| 1            | osx          | 0     | 6      | e3          |
| 3            | osx          | 0     | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;
        let left = Arc::new(Column::new_with_schema("v", &schema).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
        let f = Arc::new(BinaryExpr::new(left, Operator::Eq, right));
        let seg1 = Aggregate::<i64, i128, boolean_op::Eq>::new(
            f.clone(),
            Column::new_with_schema("v", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Column::new_with_schema("ts", &schema).unwrap(),
            1,
            TimeRange::None,
            None,
            "1".to_string(),
        );

        let seg1 = Arc::new(SegmentExprWrap::new(Box::new(seg1)));
        let seg2 = Aggregate::<i64, i128, boolean_op::Eq>::new(
            f,
            Column::new_with_schema("v", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Column::new_with_schema("ts", &schema).unwrap(),
            2,
            TimeRange::None,
            None,
            "2".to_string(),
        );
        let seg2 = Arc::new(SegmentExprWrap::new(Box::new(seg2)));
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
        let fexpr = Arc::new(FunnelExprWrap::new(f));

        let aggexpr = aggregate::Aggregate::<i64, i128>::new(
            None,
            Column::new_with_schema("v", &schema).unwrap(),
            AggregateFunction::new_sum(),
            "n1".to_string(),
        );
        let seg = SegmentedAggregateExpr::try_new(
            vec![
                Arc::new(Column::new_with_schema("user_id", &schema)?),
                Arc::new(Column::new_with_schema("device", &schema)?),
            ],
            vec![seg1, seg2],
            vec![fexpr, Arc::new(aggexpr)],
            Arc::new(input),
            1,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }
}
