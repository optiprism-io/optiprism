use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::Result;

#[derive(Debug)]
pub struct ReorderColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<String>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl ReorderColumnsExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, columns: Vec<String>) -> Result<Self> {
        let schema = input.schema();

        let mut reordered_cols = vec![];

        for group_col in columns.iter() {
            reordered_cols.push(schema.field_with_name(group_col).unwrap().to_owned());
        }
        for field in schema.fields().iter() {
            if !columns.contains(field.name()) {
                reordered_cols.push(field.deref().to_owned());
            }
        }

        let schema = Arc::new(Schema::new(reordered_cols));
        let cache = Self::compute_properties(&input, schema.clone())?;
        Ok(Self {
            input,
            columns,
            schema,
            cache,
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for ReorderColumnsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ReorderColumnsExec")
    }
}

#[async_trait]
impl ExecutionPlan for ReorderColumnsExec {
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
            ReorderColumnsExec::try_new(children[0].clone(), self.columns.clone())
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(ReorderColumnsStream {
            stream,
            schema: self.schema.clone(),
        }))
    }
}
struct ReorderColumnsStream {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl Stream for ReorderColumnsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let cols = self
                    .schema
                    .fields
                    .iter()
                    .map(|f| batch.column_by_name(f.name()).unwrap().to_owned())
                    .collect::<Vec<_>>();

                Poll::Ready(Some(Ok(RecordBatch::try_new(self.schema.clone(), cols)?)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for ReorderColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
