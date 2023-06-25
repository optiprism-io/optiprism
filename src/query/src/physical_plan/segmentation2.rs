use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
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
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
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
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
pub struct SegmentationExec {
    predicate: Arc<dyn SegmentationExpr>,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    out_batch_size: usize,
}

impl SegmentationExec {
    pub fn try_new(
        predicate: Arc<dyn SegmentationExpr>,
        partition_key: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let res = Self {
            predicate,
            input,
            metrics: Default::default(),
            partition_key,
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
        self.input.schema().clone()
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
                self.predicate.clone(),
                self.partition_key.clone(),
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
            predicate: self.predicate.clone(),
            partition_key: self.partition_key.clone(),
            input: self.input.execute(partition, context.clone())?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            is_ended: false,
            out_batch_size: self.out_batch_size,
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
    predicate: Arc<dyn SegmentationExpr>,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    is_ended: bool,
    out_batch_size: usize,
}

impl Stream for SegmentationStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            println!("nothing to poll");
            return Poll::Ready(None);
        }

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        let mut batch_buf: VecDeque<RecordBatch> = VecDeque::with_capacity(100);
        let mut spans_buf: VecDeque<Vec<i64>> = VecDeque::with_capacity(100);
        let mut res_buf: VecDeque<Option<BooleanArray>> = VecDeque::with_capacity(100);
        let mut out_buf: VecDeque<RecordBatch> = VecDeque::with_capacity(100);
        // let mut span_buf: VecDeque<Vec<usize>> = Default::default();

        let mut last_hash = 0;
        // let mut offset = 0;
        loop {

                let mut spans = vec![];
                let res = match self.input.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        batch_buf.push_back(batch.clone());
                        if batch_buf.len()>2 {
                            batch_buf.pop_front();
                        }
                        // println!("get batch: {:?}", batch);
                        let num_rows = batch.num_rows();
                        hash_buf.resize(num_rows, 0);
                        let pk_arrs = self
                            .partition_key
                            .iter()
                            .map(|v| v.evaluate(&batch))
                            .collect::<std::result::Result<Vec<ColumnarValue>, _>>()?
                            .iter()
                            .map(|v| v.clone().into_array(batch.num_rows()).clone())
                            .collect::<Vec<ArrayRef>>();
                        create_hashes(&pk_arrs, &mut random_state, &mut hash_buf).unwrap();
                        let res = self
                            .predicate
                            .evaluate(&batch, &hash_buf)
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        let res = res.map(|v| v.as_any().downcast_ref::<BooleanArray>().unwrap().clone());
                        res_buf.push_back(res.clone());
                        if res_buf.len()>2 {
                            res_buf.pop_front();
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

                        spans_buf.push_back(spans);
                        if spans_buf.len()>2 {
                            spans_buf.pop_front();
                        }
                        res
                    }
                    Poll::Ready(None) => {
                        // println!("no more batches");
                        let res = self
                            .predicate
                            .finalize()
                            .map_err(|e| e.into_datafusion_execution_error())?;
                        let res = res.as_any().downcast_ref::<BooleanArray>().unwrap().clone();
                        res_buf.push_back(Some(res.clone()));
                        if res_buf.len()>2 {
                            res_buf.pop_front();
                        }
                        self.is_ended = true;
                        Some(res)
                    }
                    other => return other,
                };

                if res.is_some() {
                    println!("batch buf: {:?}", batch_buf.iter().map(|v|v.columns()[0].clone()).collect::<Vec<_>>());
                    println!("span buf: {:?}", spans_buf);
                    println!("res buf: {:?}", res_buf);
                    let res = res_buf.pop_back().unwrap();
                    let spans = spans_buf.pop_back().unwrap();
                    let filtered_spans = spans.into_iter().zip(res.unwrap().iter()).filter_map(|(v,b)|{
                        match b.unwrap() {
                            true => Some(v),
                            false => None
                        }
                    }).collect::<Vec<_>>();
                    println!("filtered spans: {:?}", filtered_spans);
                }

            if self.is_ended {
                break;
            }
            // println!("spans orig: {:?}", spans);

            /*match res {
                None => {}

                Some(arr) => {
                    let arrb = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
                    println!("{:?}", arr);
                    let prev_batch = batch_buf.pop_back().unwrap();
                    if arrb.value(0) {
                        let to_take = Int64Array::from(vec![prev_batch.num_rows() as i64 - 1]);
                        let res1 = take(prev_batch.columns()[0].as_ref(), &to_take, None)?;
                        // println!("r1 {:?}", res1);
                    }

                    if spans.len() > 0 {
                        let spans = spans[..spans.len() - 1]
                            .iter()
                            .zip(arrb.iter())
                            .filter_map(|(offset, keep)| match keep {
                                Some(true) => Some(*offset),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        // println!("spans {:?}", spans);
                        let to_take = Int64Array::from(spans);
                        let res_arrs = batch
                            .columns()
                            .iter()
                            .map(|c| take(c, &to_take, None))
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        let res_batch = RecordBatch::try_new(
                            self.schema().clone(),
                            res_arrs.into_iter().collect::<Vec<_>>(),
                        )?;
                        out_buf.push_back(res_batch);

                        // println!("r2 {:?}", res2);
                    }
                    // println!("prev batch {:?}", prev_batch);
                    // println!("batch {:?}", batch);
                }
            }

            let rows = out_buf.iter().map(|b| b.num_rows()).sum::<usize>();
            // return buffer only if it's full or we are at the end
            if rows > self.out_batch_size || self.is_ended {
                println!("eeee");
                let rb = concat_batches(
                    &self.schema(),
                    out_buf.iter().map(|v| v).collect::<Vec<_>>(),
                )?;
                return self.baseline_metrics.record_poll(Poll::Ready(Some(Ok(rb))));
            }*/
        }

        return Poll::Ready(None);
    }
}
impl RecordBatchStream for SegmentationStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
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
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use segmentation::SegmentationExpr;

    use crate::physical_plan::expressions::segmentation;
    use crate::physical_plan::expressions::segmentation::comparison;
    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::AggregateFunction;
    use crate::physical_plan::segmentation2::SegmentationExec;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]));
        let v = vec![
            vec![1, 1, 1],
            vec![1, 1, 1],
            vec![1, 1, 1],
            vec![1, 3, 4, 5, 5, 5, 6, 6, 7],
            vec![8, 8, 9, 10],
            vec![11, 11, 11, 11],
            vec![11, 11, 11, 11],
            vec![11, 11],
            vec![11, 12, 13, 14],
        ];

        let batches = v
            .into_iter()
            .map(|v| {
                let col: ArrayRef = Arc::new(Int64Array::from(v));
                RecordBatch::try_new(schema.clone(), vec![col.clone()]).unwrap()
            })
            .collect::<Vec<_>>();

        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let mut agg = Count::new();

        let agg = comparison::Eq::new(Arc::new(agg), ScalarValue::Int64(Some(1)));
        let segmentation = SegmentationExec::try_new(
            Arc::new(agg),
            vec![Arc::new(Column::new_with_schema("col", &schema)?)],
            Arc::new(input),
            111,
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = segmentation.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        println!("{}", pretty_format_batches(&result)?);
        Ok(())
    }
}
