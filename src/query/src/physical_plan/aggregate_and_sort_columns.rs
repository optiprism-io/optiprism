use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::RecordBatch;
use arrow::compute::sort_to_indices;
use arrow::compute::take;
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties};
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

#[derive(Debug, Clone)]
enum Agg {
    Sum(i128),
    Avg(i128, i128),
}

impl Agg {
    pub fn result(&self) -> i128 {
        match self {
            Agg::Sum(sum) => *sum,
            Agg::Avg(sum, count) => *sum / *count,
        }
    }
}

#[derive(Debug)]
pub struct AggregateAndSortColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    groups: usize,
    cache: PlanProperties,
    schema: SchemaRef,
}

impl AggregateAndSortColumnsExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, groups: usize) -> Result<Self> {
        let schema = input.schema();

        let mut cols = vec![];

        for (idx, f) in schema.fields.iter().enumerate() {
            cols.push(f.to_owned());
            if idx == groups - 1 {
                let col = Arc::new(Field::new(
                    "Average",
                    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                    false,
                ));
                cols.push(col);
            }
        }
        let schema = Arc::new(Schema::new(cols));
        let cache = Self::compute_properties(&input,schema.clone())?;

        Ok(Self {
            input,
            groups,
            cache,
            schema,
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>,schema:SchemaRef) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for AggregateAndSortColumnsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateAndSortColumnsExec")
    }
}

#[async_trait]
impl ExecutionPlan for AggregateAndSortColumnsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
            AggregateAndSortColumnsExec::try_new(children[0].clone(), self.groups.clone())
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
            schema: self.schema.clone(),
            groups: self.groups,
        }))
    }
}
struct AggregateAndSortColumnsStream {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    groups: usize,
}

impl Stream for AggregateAndSortColumnsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let mut res = Decimal128Builder::with_capacity(batch.num_rows());
                let arrs = batch
                    .columns()
                    .iter()
                    .skip(self.groups)
                    .map(|col| col.as_any().downcast_ref::<Decimal128Array>().unwrap())
                    .collect::<Vec<_>>();

                for row_id in 0..batch.num_rows() {
                    let mut v = 0;
                    let mut c = 0;
                    for arr_id in 0..arrs.len() {
                        v += arrs[arr_id].value(row_id);
                        c += 1;
                    }
                    res.append_value(v / c);
                }
                let avg = res
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let idx = Arc::new(sort_to_indices(
                    &avg,
                    Some(SortOptions {
                        descending: true,
                        nulls_first: false,
                    }),
                    None,
                )?) as ArrayRef;

                let mut out = vec![];
                for (idx, column) in batch.columns().into_iter().enumerate() {
                    out.push(column.to_owned());
                    if idx == self.groups - 1 {
                        out.push(Arc::new(avg.clone()));
                    }
                }

                let out = out
                    .iter()
                    .map(|c| take(c, &idx, None).unwrap())
                    .collect::<Vec<_>>();

                let rb = RecordBatch::try_new(self.schema.clone(), out)?;

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

    use crate::physical_plan::aggregate_and_sort_columns::AggregateAndSortColumnsExec;

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

        let exec = AggregateAndSortColumnsExec::try_new(Arc::new(input), 2).unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let result = collect(stream).await.unwrap();

        print_batches(&result).unwrap();
    }
}
