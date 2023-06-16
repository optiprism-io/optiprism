use std::any::Any;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Int64Array;
use arrow::array::TimestampMillisecondBuilder;
use arrow::array::UInt32Array;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Builder;
use arrow::array::UInt8Builder;
use arrow::compute::concat;
use arrow::compute::filter;
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow::error::ArrowError;
use arrow::error::Result as ArrowResult;
use arrow::ipc::TimestampBuilder;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use axum::extract::State;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
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
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::expressions::funnel::FunnelExpr;
use crate::physical_plan::expressions::funnel::FunnelResult;
use crate::physical_plan::PartitionState;
use crate::Result;
use crate::DEFAULT_BATCH_SIZE;

pub struct FunnelExec {
    predicate: FunnelExpr,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
}

impl FunnelExec {
    pub fn try_new(
        predicate: FunnelExpr,
        partition_key: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = {
            let mut fields = vec![];
            fields.push(Field::new("is_converted", DataType::Boolean, true));
            fields.push(Field::new("converted_steps", DataType::UInt8, true));
            for step_id in 0..=predicate.steps_count() {
                fields.push(Field::new(
                    format!("step_{step_id}_ts"),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ));
            }

            Arc::new(Schema::new(
                [fields, input.schema().fields.clone()].concat(),
            ))
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
            FunnelExec::try_new(
                self.predicate.clone(),
                self.partition_key.clone(),
                children[0].clone(),
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
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
            // out_buf: Default::default(),
            state: PartitionState::new(self.partition_key.clone()),
            partition_key: self.partition_key.clone(),
            // is_completed_col: HashMap::new(),
            // converted_steps_col: HashMap::new(),
            // converted_by_step_col: HashMap::new(),
            // to_take: HashMap::default(),
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
    state: PartitionState,
    // out_buf: VecDeque<RecordBatch>,
    // is_completed_col: HashMap<usize, BooleanBuilder>,
    // converted_steps_col: HashMap<usize, UInt32Builder>,
    // converted_by_step_col: HashMap<usize, Vec<TimestampMillisecondBuilder>>,
    // to_take: HashMap<usize, Vec<usize>>,
}

#[inline]
pub fn abs_id(offset: usize, batches: &[RecordBatch]) -> (usize, usize) {
    let mut batch_id = 0;
    let mut idx = offset;
    for batch in batches {
        if idx < batch.num_rows() {
            break;
        }
        idx -= batch.num_rows();
        batch_id += 1;
    }
    (batch_id, idx)
}

#[async_trait]
impl Stream for FunnelExecStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut out_buf: VecDeque<RecordBatch> = Default::default();
        let mut is_completed_col: HashMap<usize, BooleanBuilder> = Default::default();
        let mut converted_steps_col: HashMap<usize, UInt32Builder> = Default::default();
        let mut converted_by_step_col: HashMap<usize, Vec<TimestampMillisecondBuilder>> =
            Default::default();
        let mut to_take: HashMap<usize, Vec<usize>> = Default::default();

        let mut is_ended = false;
        println!("loop");
        loop {
            let mut offset = 0;
            // let timer = self.baseline_metrics.elapsed_compute().timer();
            let res = match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    println!("pulled batch {:?}", batch);
                    if let Some((batches, spans, skip)) = self.state.push(batch)? {
                        offset = skip;
                        println!(
                            "state: spans batches {:?} {:?} offset: {skip}",
                            spans,
                            batches
                                .iter()
                                .map(|v| v.columns()[0].clone())
                                .collect::<Vec<_>>()
                        );
                        let ev_res = self
                            .predicate
                            .evaluate(&batches, spans.clone(), skip)
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        println!("ev result: {:?}", ev_res);
                        Some((batches, spans, ev_res))
                    } else {
                        println!("possible more to push");
                        None
                    }
                }
                Poll::Ready(None) => {
                    println!("last");
                    is_ended = true;
                    if let Some((batches, spans, skip)) = self.state.finalize()? {
                        offset = skip;
                        println!("final state: batches spans {:?} {:?}", batches, spans);
                        let ev_res = self
                            .predicate
                            .evaluate(&batches, spans.clone(), skip)
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        Some((batches, spans, ev_res))
                    } else {
                        println!("no final");
                        None
                    }
                }
                other => return other,
            };

            if let Some((batches, spans, ev_res)) = res {
                for (span, fr) in spans.into_iter().zip(ev_res.into_iter()) {
                    let (batch_id, row_id) = abs_row_id(offset, &batches);
                    to_take.entry(batch_id).or_default().push(row_id);
                    match fr {
                        FunnelResult::Completed(steps) => {
                            is_completed_col
                                .entry(batch_id)
                                .or_default()
                                .append_value(true);
                            converted_steps_col
                                .entry(batch_id)
                                .or_default()
                                .append_value(steps.len() as u32);
                            let steps_len = self.predicate.steps_count();
                            for (id, step) in steps.iter().enumerate() {
                                converted_by_step_col.entry(batch_id).or_insert_with(|| {
                                    (0..steps_len)
                                        .into_iter()
                                        .map(|_| TimestampMillisecondBuilder::new())
                                        .collect::<Vec<_>>()
                                })[id]
                                    .append_value(step.ts);
                            }
                        }
                        FunnelResult::Incomplete(steps, stepn) => {
                            is_completed_col
                                .entry(batch_id)
                                .or_default()
                                .append_value(false);
                            converted_steps_col
                                .entry(batch_id)
                                .or_default()
                                .append_value(stepn as u32);
                            let steps_len = self.predicate.steps_count();
                            for step in 0..steps_len {
                                if step < steps_len {
                                    converted_by_step_col.entry(batch_id).or_insert_with(|| {
                                        (0..steps_len)
                                            .into_iter()
                                            .map(|_| TimestampMillisecondBuilder::new())
                                            .collect::<Vec<_>>()
                                    })[step]
                                        .append_value(steps[step].ts);
                                } else {
                                    converted_by_step_col.entry(batch_id).or_insert_with(|| {
                                        (0..steps_len)
                                            .into_iter()
                                            .map(|_| TimestampMillisecondBuilder::new())
                                            .collect::<Vec<_>>()
                                    })[step]
                                        .append_null();
                                }
                            }
                        }
                    }
                    offset += span;
                }

                let mut batches = to_take
                    .iter()
                    .map(|(batch_id, rows)| {
                        let take_arr = rows.iter().map(|b| Some(*b as u32)).collect::<Vec<_>>();
                        let take_arr = UInt32Array::from(take_arr);
                        let cols = batches[*batch_id]
                            .columns()
                            .iter()
                            .map(|arr| take(arr, &take_arr, None))
                            .collect::<std::result::Result<Vec<_>, _>>()?;

                        let is_completed = is_completed_col.remove(&batch_id).unwrap().finish();
                        let is_completed = Arc::new(is_completed) as ArrayRef;
                        let converted_steps =
                            converted_steps_col.remove(&batch_id).unwrap().finish();
                        let converted_steps = Arc::new(converted_steps) as ArrayRef;
                        let converted_by_step = converted_by_step_col
                            .remove(&batch_id)
                            .unwrap()
                            .iter_mut()
                            .map(|v| v.finish())
                            .collect::<Vec<_>>();
                        let converted_by_step = converted_by_step
                            .into_iter()
                            .map(|v| Arc::new(v) as ArrayRef)
                            .collect::<Vec<_>>();
                        println!("final");
                        println!("schema {:?}", self.schema.clone());
                        println!(" {}", self.schema.fields().len());
                        let arrs = vec![
                            vec![is_completed],
                            vec![converted_steps],
                            converted_by_step,
                            cols,
                        ]
                        .concat();
                        println!("{:?}", arrs);
                        println!("len {}", arrs.len());
                        RecordBatch::try_new(self.schema.clone(), arrs)
                    })
                    .collect::<Vec<std::result::Result<_, _>>>()
                    .into_iter()
                    .collect::<std::result::Result<Vec<RecordBatch>, _>>()?;

                out_buf.append(&mut batches.into());
                // for batch in batches.into_iter() {
                // print!("{}", arrow::util::pretty::pretty_format_batches(&[batch]).unwrap());
                // }
            }

            let rows = out_buf.iter().map(|b| b.num_rows()).sum::<usize>();
            if rows > 3 || is_ended {
                let arrs = self
                    .schema
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(idx, _)| {
                        let to_concat = out_buf
                            .iter()
                            .map(|b| b.column(idx).clone())
                            .collect::<Vec<_>>();
                        concat(
                            to_concat
                                .iter()
                                .map(|v| v.as_ref())
                                .collect::<Vec<_>>()
                                .as_ref(),
                        )
                    })
                    .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

                let rb = RecordBatch::try_new(self.schema.clone(), arrs)
                    .map_err(|err| DataFusionError::from(err))?;
                return self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(rb))));
            }

            // timer.done();
        }
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

    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;

    use crate::error::Result;
    use crate::event_eq;
    use crate::physical_plan::expressions::funnel::test_utils::event_eq_;
    use crate::physical_plan::expressions::funnel::Count;
    use crate::physical_plan::expressions::funnel::FunnelExpr;
    use crate::physical_plan::expressions::funnel::Options;
    use crate::physical_plan::expressions::funnel::StepOrder::Sequential;
    use crate::physical_plan::expressions::funnel::Touch;
    use crate::physical_plan::funnel::FunnelExec;
    use crate::physical_plan::merge::MergeExec;
    use crate::physical_plan::PartitionState;

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
                arrow2::datatypes::Field::new(
                    "ts",
                    arrow2::datatypes::DataType::Timestamp(
                        arrow2::datatypes::TimeUnit::Millisecond,
                        None,
                    ),
                    false,
                ),
                arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
                arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
            ];
            let res = parse_markdown_table(data, &fields).unwrap();

            let (arrs, fields) = res
                .into_iter()
                .zip(fields)
                .map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap())
                .unzip();

            let schema = Arc::new(Schema::new(fields)) as SchemaRef;

            (arrs, schema)
        };

        let opts = Options {
            ts_col: Column::new_with_schema("ts", &schema)?,
            window: Duration::seconds(15),
            steps: event_eq!(schema, "e1" Sequential, "e2" Sequential),
            exclude: None,
            constants: None,
            count: Count::Unique,
            filter: None,
            touch: Touch::First,
        };

        let batches = {
            let batch = RecordBatch::try_new(schema.clone(), arrs)?;
            let to_take = vec![4, 9, 9, 1, 1, 1, 3, 3];
            let mut offset = 0;
            to_take
                .into_iter()
                .map(|v| {
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

        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let funnel = FunnelExec::try_new(
            FunnelExpr::new(opts),
            vec![Arc::new(Column::new_with_schema("user_id", &schema)?)],
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
