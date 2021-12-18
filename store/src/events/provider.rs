use std::any::Any;
use std::sync::Arc;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

/*pub struct Provider {

}

impl TableProvider for Provider {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn schema(&self) -> SchemaRef {
        todo!()
    }

    async fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize, filters: &[Expr], limit: Option<usize>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

*/