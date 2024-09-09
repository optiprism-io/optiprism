use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Array;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;
use crate::error::QueryError;
use crate::Result;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum Value {
    Int64(Option<i64>),
    Decimal(Option<i128>),
    String(Option<String>),
}

#[derive(Debug)]
pub struct LimitGroupsExec {
    input: Arc<dyn ExecutionPlan>,
    groups: usize,
    skip_cols: usize,
    limit: usize,
    cache: PlanProperties,
}

impl LimitGroupsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        skip: usize,
        groups: usize,
        limit: usize,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            input,
            skip_cols: skip,
            groups,
            limit,
            cache,
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> Result<PlanProperties> {
        let eq_properties = input.equivalence_properties().clone();

        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for LimitGroupsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LimitGroupsExec")
    }
}

#[async_trait]
impl ExecutionPlan for LimitGroupsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            LimitGroupsExec::try_new(children[0].clone(), self.skip_cols, self.groups, self.limit)
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(AggregateAndSortColumnsStream {
            stream,
            schema: self.input.schema(),
            groups: self.groups,
            skip: self.skip_cols,
            limit: self.limit,
        }))
    }
}
struct AggregateAndSortColumnsStream {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    groups: usize,
    skip: usize,
    limit: usize,
}

impl Stream for AggregateAndSortColumnsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if self.groups == 0 {
                    return Poll::Ready(Some(Ok(batch)));
                }
                let arrs = batch
                    .columns()
                    .iter()
                    .skip(self.skip) // skip segment col
                    .take(self.groups) // get all groups, ignore agg_name
                    .zip(batch.schema().fields().iter().skip(self.skip))
                    .map(|(col, f)| match f.data_type() {
                        DataType::Int64 => col
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .iter()
                            .map(|v| Value::Int64(v))
                            .collect::<Vec<_>>(),
                        DataType::Decimal128(_, _) => col
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .iter()
                            .map(|v| Value::Decimal(v))
                            .collect::<Vec<_>>(),
                        DataType::Utf8 => col
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .iter()
                            .map(|v| Value::String(v.map(|v| v.to_string())))
                            .collect::<Vec<_>>(),
                        _ => unimplemented!(),
                    })
                    .collect::<Vec<_>>();

                let mut counters: Vec<HashMap<Value, usize>> =
                    vec![HashMap::default(); self.groups];

                let mut to_take = Int64Builder::with_capacity(batch.num_rows());
                'c: for row_id in 0..batch.num_rows() {
                    for group_id in 0..self.groups {
                        let value = arrs[group_id][row_id].clone();
                        let cnt = counters[group_id].entry(value.clone()).or_insert_with(|| 0);
                        if *cnt == self.limit {
                            continue 'c;
                        }
                        if let Some(v) = counters[group_id].get_mut(&value) {
                            *v += 1;
                        }
                    }
                    to_take.append_value(row_id as i64);
                }
                let to_take_arr = Arc::new(to_take.finish()) as ArrayRef;
                let cols = batch
                    .columns()
                    .iter()
                    .map(|c| take(&c, &to_take_arr, None))
                    .collect::<arrow::error::Result<Vec<_>>>()?;
                let rb = RecordBatch::try_new(self.stream.schema().clone(), cols)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                Poll::Ready(Some(Ok(rb)))
            }
            other => return other,
        }
    }
}

impl RecordBatchStream for AggregateAndSortColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use storage::test_util::parse_markdown_tables;

    
    use crate::physical_plan::limit_groups::LimitGroupsExec;

    #[tokio::test]
    async fn it_works() {
        let data = r#"
| a(i64) | b(i64) | c(decimal) | d(decimal) |
|--------|--------|---------|--------|
| 1      | 1      | 1       | 2      |
| 1      | 2      | 2       | 3      |
| 1      | 3      | 3       | 3      |
| 1      | 4      | 4       | 3      |
| 2      | 1      | 5       | 4      |
| 2      | 2      | 6       | 2      |
| 2      | 3      | 7       | 2      |
| 2      | 4      | 8       | 2      |
| 3      | 1      | 9       | 2      |
| 3      | 2      | 10      | 2      |
"#;

        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let input = MemoryExec::try_new(&[res], schema.clone(), None).unwrap();

        let exec = LimitGroupsExec::try_new(Arc::new(input), 0, 2, 2).unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let result = collect(stream).await.unwrap();

        print_batches(&result).unwrap();
    }
}
