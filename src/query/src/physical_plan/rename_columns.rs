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
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::merge::MergeExec;
use crate::Result;

#[derive(Debug)]
pub struct RenameColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    columns: Vec<(String, String)>,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl RenameColumnsExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, columns: Vec<(String, String)>) -> Result<Self> {
        let schema = input.schema();

        let fields = schema
            .fields
            .iter()
            .map(|f| {
                for col in columns.iter() {
                    if f.name() == &col.0 {
                        return arrow::datatypes::Field::new(
                            &col.1,
                            f.data_type().clone(),
                            f.is_nullable(),
                        );
                    }
                }

                return f.deref().clone();
            })
            .collect::<Vec<_>>();
        let schema= Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(&input,schema.clone())?;
        Ok(Self {
            input,
            columns,
            schema,
            cache,
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema:SchemaRef) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);

        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for RenameColumnsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RenameColumnsExec")
    }
}

#[async_trait]
impl ExecutionPlan for RenameColumnsExec {
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
            RenameColumnsExec::try_new(children[0].clone(), self.columns.clone())
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(RenameColumnsStream {
            stream,
            columns: self.columns.clone(),
            schema: self.schema.clone(),
        }))
    }
}

struct RenameColumnsStream {
    stream: SendableRecordBatchStream,
    columns: Vec<(String, String)>,
    schema: SchemaRef,
}

impl Stream for RenameColumnsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        return match self.stream.poll_next_unpin(cx) {
            v => v,
        };
    }
}

impl RecordBatchStream for RenameColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
