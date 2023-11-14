use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion_common::Statistics;
use store::db::OptiDB;
use crate::error::Result;

#[derive(Debug)]
pub struct LocalExec {
    schema: SchemaRef,
    db: Arc<OptiDB>,
}

impl LocalExec {
    pub fn try_new(schema: SchemaRef, db: Arc<OptiDB>) -> Result<Self> {
        Ok(Self {
            schema,
            db,
        })
    }
}

impl ExecutionPlan for LocalExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        todo!()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}