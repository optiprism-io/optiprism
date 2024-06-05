use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::Partitioning;
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
pub struct RenameColumnRowsExec {
    input: Arc<dyn ExecutionPlan>,
    column: Column,
    rename: Vec<(String, String)>,
    cache: PlanProperties,
}

impl RenameColumnRowsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column: Column,
        rename: Vec<(String, String)>,
    ) -> Result<RenameColumnRowsExec> {
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            input,
            column,
            rename,
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

impl DisplayAs for RenameColumnRowsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RenameColumnRowsExec")
    }
}

#[async_trait]
impl ExecutionPlan for RenameColumnRowsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema().clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(RenameColumnRowsExec::new(
            children[0].clone(),
            self.column.clone(),
            self.rename.to_vec(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        Ok(Box::pin(RenameColumnRowsStream {
            stream,
            column: self.column.clone(),
            rename: self.rename.to_vec(),
        }))
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
}

struct RenameColumnRowsStream {
    stream: SendableRecordBatchStream,
    column: Column,
    rename: Vec<(String, String)>,
}

impl Stream for RenameColumnRowsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let arrs = batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(batch.columns().iter())
                    .map(|(field, arr)| {
                        if field.name().as_str() == self.column.name() {
                            let mut b = StringBuilder::new();
                            for v in arr.as_any().downcast_ref::<StringArray>().unwrap() {
                                match v {
                                    None => b.append_null(),
                                    Some(name) => {
                                        match self
                                            .rename
                                            .iter()
                                            .find(|(from, to)| name == from.as_str())
                                        {
                                            None => b.append_option(v),
                                            Some((_, to)) => b.append_value(to.to_owned()),
                                        }
                                    }
                                }
                            }
                            Arc::new(b.finish()) as ArrayRef
                        } else {
                            arr.to_owned()
                        }
                    })
                    .collect::<Vec<_>>();
                return Poll::Ready(Some(Ok(RecordBatch::try_new(
                    self.stream.schema().clone(),
                    arrs,
                )
                .unwrap())));
            }
            other => other,
        }
    }
}

impl RecordBatchStream for RenameColumnRowsStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema().clone()
    }
}
