use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use store::db::{OptiDB, OptiDBImpl};
use crate::error::Result;
use crate::physical_plan::local::LocalExec;
use datafusion::error::DataFusionError;

pub struct LocalTable {
    schema: SchemaRef,
    db: Arc<OptiDBImpl>,
    partitions: usize,
}

impl LocalTable {
    pub fn try_new(db: Arc<OptiDBImpl>, schema: SchemaRef, partitions: usize) -> Result<Self> {
        Ok(Self {
            schema,
            db,
            partitions,
        })
    }
}

#[async_trait]
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
        let streams = self.db.scan(self.partitions, vec![]).map_err(|e| DataFusionError::Internal(e.to_string()))?;
        Ok(Arc::new(LocalExec::try_new(self.schema.clone(), streams).map_err(|e| e.into_datafusion_execution_error())?) as Arc<dyn ExecutionPlan>)
    }
}