use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::{ArrayBuilder, ArrayRef, BooleanBuilder, Int64Array, TimestampMillisecondBuilder, UInt64Builder, UInt8Builder};
use arrow::compute::{concat, filter};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::ipc::TimestampBuilder;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use axum::extract::State;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use super::expressions::funnel::FunnelExpr;
use crate::error::QueryError;
use crate::physical_plan::expressions::funnel::FunnelResult;
use crate::physical_plan::PartitionState;
use crate::{DEFAULT_BATCH_SIZE, Result};

pub struct FunnelExec {
    predicate: FunnelExpr,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
}

impl FunnelExec {
    pub fn try_new(predicate: FunnelExpr, partition_key: Vec<Arc<dyn PhysicalExpr>>, input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let schema = {
            let mut fields = vec![];
            fields.push(Field::new("is_converted", DataType::Boolean, true));
            fields.push(Field::new("converted_steps", DataType::UInt8, true));
            for step_id in 0..=predicate.steps_count() {
                fields.push(Field::new(format!("step_{step_id}_ts"), DataType::Timestamp(TimeUnit::Millisecond, None), true));
            }

            Arc::new(Schema::new([fields, input.schema().fields.clone()].concat()))
        };

        Ok(Self {
            predicate,
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            partition_key,
        })
    }
}

impl Debug for FunnelExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FunnelExec")
    }
}

#[async_trait]
impl ExecutionPlan for FunnelExec {
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
            FunnelExec::try_new(self.predicate.clone(), self.partition_key.clone(), children[0].clone()).map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        Ok(Box::pin(FunnelExecStream {
            predicate: self.predicate.clone(),
            input: self.input.execute(partition, context.clone())?,
            schema: self.schema.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            partition_key: self.partition_key.clone(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FunnelExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct FunnelExecStream {
    predicate: FunnelExpr,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    baseline_metrics: BaselineMetrics,
}

#[async_trait]
impl Stream for FunnelExecStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = PartitionState::new(self.partition_key.clone());
        let mut converted_steps = UInt64Builder::with_capacity(DEFAULT_BATCH_SIZE);
        let mut steps_ts = (0..self.predicate.steps_count()).into_iter().map(|_| TimestampMillisecondBuilder::with_capacity(DEFAULT_BATCH_SIZE)).collect::<Vec<_>>();
        let mut to_filter = BooleanBuilder::with_capacity(DEFAULT_BATCH_SIZE);
        let num_cols = self.schema.fields().len() - 1 - self.predicate.steps_count();
        let mut pre_batch_res: Vec<Vec<ArrayRef>> = vec![vec![]; num_cols];

        let mut is_ended = false;

        while !is_ended {
            // let timer = self.baseline_metrics.elapsed_compute().timer();
            let res = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    if let Some((batches, spans)) = state.push(batch)? {
                        let ev_res = self.predicate.evaluate(&batches, spans.clone()).map_err(|e| e.into_datafusion_execution_error())?;
                        Some((batches, spans, ev_res))
                    } else {
                        None
                    }
                }
                Poll::Ready(None) => {
                    is_ended = true;
                    if let Some((batches, spans)) = state.finalize()? {
                        let ev_res = self.predicate.evaluate(&batches, spans.clone()).map_err(|e| e.into_datafusion_execution_error())?;
                        Some((batches, spans, ev_res))
                    } else {
                        None
                    }
                }
                other => return other,
            };

            if let Some((batches, spans, res)) = res {
                let mut offset = 0;
                let mut batch_iter = batches.into_iter().peekable();
                for (span, funnel_result) in spans.into_iter().zip(res.into_iter()) {
                    match funnel_result {
                        FunnelResult::Completed(steps) => {
                            converted_steps.append_value(steps.len() as u64);

                            for (step_id, step) in steps.into_iter().enumerate() {
                                steps_ts[step_id].append_value(step.ts as i64);
                            }
                        }
                        FunnelResult::Incomplete(steps, stepn) => {
                            converted_steps.append_value(0);
                            for step_id in 0..self.predicate.steps_count() {
                                if step_id < stepn {
                                    steps_ts[step_id].append_value(steps[step_id].ts as i64);
                                } else {
                                    steps_ts[step_id].append_null();
                                }
                            }
                        }
                    }
                    to_filter.append_value(true);
                    for _ in 0..span {
                        to_filter.append_value(false);
                    }
                    offset += span;
                    if offset > batch_iter.peek().unwrap().num_rows() {
                        let fb = to_filter.finish();
                        let batch = batch_iter.next().unwrap();
                        let cols = batch.columns().iter().map(|col| filter(col, &fb)).collect::<ArrowResult<Vec<_>>>()?;
                        for (col_id, col) in cols.into_iter().enumerate() {
                            pre_batch_res[col_id].push(col);
                        }

                        if batch_iter.next().is_none() {
                            break;
                        }
                    }

                    if converted_steps.len() >= DEFAULT_BATCH_SIZE {
                        let converted_col = converted_steps.finish();
                        let step_ts_cols = steps_ts.iter_mut().map(|v| v.finish()).collect::<Vec<_>>();
                        let cols = pre_batch_res.iter().map(|cols| {
                            let to_concat = cols.into_iter().map(|v| v.as_ref()).collect::<Vec<_>>();
                            concat(to_concat.as_slice())
                        }).collect::<ArrowResult<Vec<_>>>()?;
                        let mut res_cols = vec![];
                        res_cols.push(Arc::new(converted_col) as ArrayRef);
                        for col in step_ts_cols.into_iter() {
                            res_cols.push(Arc::new(col) as ArrayRef)
                        }
                        for col in cols.into_iter() {
                            res_cols.push(col)
                        }

                        let batch = RecordBatch::try_new(self.schema.clone(), res_cols)?;

                        let poll = Poll::Ready(Some(Ok(batch)));
                        return self.baseline_metrics.record_poll(poll);
                    }
                }
            }

            // timer.done();
        }

        let poll = if !converted_steps.is_empty() {
            let converted_col = converted_steps.finish();
            let step_ts_cols = steps_ts.iter_mut().map(|v| v.finish()).collect::<Vec<_>>();
            let cols = pre_batch_res.iter().map(|cols| {
                let to_concat = cols.into_iter().map(|v| v.as_ref()).collect::<Vec<_>>();
                concat(to_concat.as_slice())
            }).collect::<ArrowResult<Vec<_>>>()?;
            let mut res_cols = vec![];
            res_cols.push(Arc::new(converted_col) as ArrayRef);
            for col in step_ts_cols.into_iter() {
                res_cols.push(Arc::new(col) as ArrayRef)
            }
            for col in cols.into_iter() {
                res_cols.push(col)
            }

            let batch = RecordBatch::try_new(self.schema.clone(), res_cols)?;

            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        };

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FunnelExecStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;
    use crate::physical_plan::expressions::funnel::{Count, FunnelExpr, Options, Touch};
    use crate::physical_plan::funnel::FunnelExec;
    use crate::physical_plan::merge::MergeExec;
    use crate::physical_plan::PartitionState;
    use crate::error::Result;
use crate::physical_plan::expressions::funnel::test_utils::result_completed;
    #[tokio::test]
    async fn test_funnel() -> anyhow::Result<()> {
        let data = r#"
| user_id | ts | event | const  |
|---------|----|-------|--------|
| 0       | 1  | e1    | 1      |
| 0       | 2  | e2    | 1      |
| 0       | 3  | e3    | 1      |
| 0       | 4  | e1    | 1      |
| 1       | 1  | e1    | 1      |
| 1       | 2  | e2    | 1      |
| 1       | 3  | e3    | 1      |
| 1       | 4  | e3    | 1      |
| 2       | 1  | e1    | 1      |
| 2       | 2  | e2    | 1      |
| 2       | 3  | e3    | 1      |
| 2       | 4  | e3    | 1      |
| 2       | 5  | e3    | 1      |
| 2       | 6  | e3    | 1      |
| 3       | 1  | e1    | 1      |
| 3       | 2  | e2    | 1      |
| 3       | 3  | e3    | 1      |
| 4       | 1  | e1    | 1      |
| 4       | 2  | e2    | 1      |
| 4       | 3  | e3    | 1      |
| 5       | 1  | e1    | 1      |
| 5       | 2  | e2    | 1      |
| 6       | 1  | e1    | 1      |
| 6       | 2  | e3    | 1      |
| 6       | 3  | e3    | 1      |
| 7       | 1  | e1    | 1      |
| 7       | 2  | e2    | 1      |
| 7       | 3  | e3    | 1      |
| 8       | 1  | e1    | 1      |
| 8       | 2  | e2    | 1      |
| 8       | 3  | e3    | 1      |
"#;
        let (arrs, schema) = {
            // todo change to arrow1
            let fields = vec![
                arrow2::datatypes::Field::new("user_id", arrow2::datatypes::DataType::Int64, false),
                arrow2::datatypes::Field::new("ts", arrow2::datatypes::DataType::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None), false),
                arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
                arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
            ];
            let res = parse_markdown_table(data, &fields).unwrap();

            let (arrs, fields) = res.
                into_iter().
                zip(fields).
                map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap()).
                unzip();

            let schema = Arc::new(Schema::new(fields)) as SchemaRef;

            (arrs, schema)
        };

        let opts = Options {
            ts_col: Column::new("ts", 0),
            window: Duration::seconds(15),
            steps: super::physical_plan::expressions::funnel::event_eq!(schema, "e1" Sequential, "e2" Sequential),
            exclude: None,
            constants: None,
            count: Count::Unique,
            filter: None,
            touch: Touch::First,
        };

        let batches = {
            let batch = RecordBatch::try_new(schema.clone(), arrs)?;
            let to_take = vec![4, 9, 9, 1, 1, 1, 3, 3];
            (0..to_take)
                .into_iter()
                .map(|v| {
                    let mut offset = 0;
                    let arrs = batch
                        .columns()
                        .iter()
                        .map(|c| c.slice(offset, v).to_owned())
                        .collect::<Vec<_>>();
                    offset += v;
                    arrs
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|v| RecordBatch::try_new(schema.clone(), v).unwrap())
                .collect::<Vec<_>>()
        };

        let input = MemoryExec::try_new(vec![batch].as_slice(), schema.clone(), None)?;

        let funnel = FunnelExec::try_new(
            FunnelExpr::new(opts),
            vec![Arc::new(Column::new("ts", 0))],
            Arc::new(input),
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = funnel.execute(0, task_ctx)?;
        let result = collect(stream).await?;
        print!("{}", arrow::util::pretty::pretty_format_batches(&result)?);

        Ok(())
    }
}