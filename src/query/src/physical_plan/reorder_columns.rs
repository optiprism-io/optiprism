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
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::merge::MergeExec;
use crate::Result;

#[derive(Debug)]
pub struct ReorderColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<String>,
    schema: SchemaRef,
}

impl ReorderColumnsExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, columns: Vec<String>) -> Self {
        let schema = input.schema();

        let mut reordered_cols = vec![];

        for group_col in columns.iter() {
            reordered_cols.push(schema.field_with_name(group_col).unwrap().to_owned());
        }
        for field in schema.fields().iter() {
            if !columns.contains(&field.name()) {
                reordered_cols.push(field.deref().to_owned());
            }
        }

        Self {
            input,
            columns,
            schema: Arc::new(Schema::new(reordered_cols)),
        }
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
        Ok(Arc::new(ReorderColumnsExec::new(
            children[0].clone(),
            self.columns.clone(),
        )))
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
            other => return other,
        }
    }
}

impl RecordBatchStream for ReorderColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
