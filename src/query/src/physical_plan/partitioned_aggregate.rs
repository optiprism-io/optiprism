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
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::Statistics;
use datafusion_expr::ColumnarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::PartitionState;

pub trait PartitionedAggregateExpr {
    fn evaluate(
        &self,
        batches: &[RecordBatch],
        spans: &[usize],
        skip: usize,
    ) -> Result<Vec<ArrayRef>>;
    fn column_names(&self) -> Vec<&str>;
}

pub struct PartitionedAggregateExec {
    group_expr: Vec<Arc<dyn PhysicalExpr>>,
    agg_expr: Vec<Arc<dyn PartitionedAggregateExpr>>,
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    out_batch_size: usize,
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
        let result = Ok(Box::pin(PartitionedAggregateStream {
            group_expr: self.group_expr.clone(),
            agg_expr: self.agg_expr.clone(),
            schema: self.schema.clone(),
            state: PartitionState::new(self.group_expr.clone()),
            is_ended: false,
            out_batch_size: self.out_batch_size.clone(),
            input: self.input.execute(partition, context.clone())?,
        }));

        result
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
    is_ended: bool,
    out_batch_size: usize,
    input: SendableRecordBatchStream,
}

impl Stream for PartitionedAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }

        let mut out_buf: VecDeque<RecordBatch> = Default::default();
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
                            .map(|expr| expr.evaluate(&batches, &spans, skip))
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
                            .map(|expr| expr.evaluate(&batches, &spans, skip))
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

                        let ev_arrs = ev_res.iter().flat_map(|v| v.to_owned()).collect::<Vec<_>>();
                        // final columns
                        let arrs = vec![ev_arrs, cols].concat();

                        // final record batch
                        RecordBatch::try_new(self.schema.clone(), arrs)
                    })
                    .collect::<Vec<std::result::Result<_, _>>>()
                    .into_iter()
                    .collect::<std::result::Result<Vec<RecordBatch>, _>>()?;

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
