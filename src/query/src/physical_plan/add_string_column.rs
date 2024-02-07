use std::any::Any;
use std::fmt::Formatter;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

#[derive(Debug)]
pub struct AddStringColumnExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    col: (String, String),
}

impl AddStringColumnExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, col: (String, String)) -> Self {
        let schema = input.schema();
        let fields = vec![
            vec![Field::new(col.0.clone(), DataType::Utf8, false)],
            schema.fields.iter().map(|f| f.deref().to_owned()).collect(),
        ]
        .concat();

        let schema = Schema::new(fields);
        Self {
            input,
            schema: Arc::new(schema),
            col,
        }
    }
}

impl DisplayAs for AddStringColumnExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "AddStringColumnExec")
    }
}

impl ExecutionPlan for AddStringColumnExec {
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
        Ok(Arc::new(AddStringColumnExec::new(
            children[0].clone(),
            self.col.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(AddStringColumnStream {
            stream: self.input.execute(partition, context)?,
            col: self.col.clone(),
            schema: self.schema.clone(),
        }))
    }
}

struct AddStringColumnStream {
    stream: SendableRecordBatchStream,
    col: (String, String),
    schema: SchemaRef,
}

impl Stream for AddStringColumnStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let v = ScalarValue::Utf8(Some(self.col.1.clone()));
                let arr = v.to_array_of_size(batch.num_rows())?;
                let new_batch = RecordBatch::try_new(
                    self.schema.clone(),
                    vec![vec![arr], batch.columns().to_vec()].concat(),
                )?;

                Poll::Ready(Some(Ok(new_batch)))
            }
            other => return other,
        }
    }
}

impl RecordBatchStream for AddStringColumnStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
