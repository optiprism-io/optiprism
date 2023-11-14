use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use store::db::OptiDB;
use crate::error::Result;
use crate::physical_plan::local::LocalExec;

pub struct LocalTable {
    schema: SchemaRef,
    db: Arc<OptiDB>,
    partitions:usize,
}

impl LocalTable {
    fn try_new(db: Arc<OptiDB>, schema:SchemaRef, partitions:usize) -> Result<Self> {
        Ok(Self {
            schema,
            db,
            partitions,
        })
    }
}

impl TableProvider for LocalTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self, _state: &SessionState, projection: Option<&Vec<usize>>, _filters: &[Expr], _limit: Option<usize>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        self.db
        LocalExec::try_new(self.schema.clone(), self.db.clone()).map_err(|e|e.into_datafusion_execution_error())?;
    }
}