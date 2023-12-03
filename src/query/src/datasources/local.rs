use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::Expr;
use datafusion_expr::TableType;
use store::db::OptiDBImpl;

use crate::error::Result;
use crate::physical_plan::local::LocalExec;

pub struct LocalTable {
    db: Arc<OptiDBImpl>,
    tbl_name: String,
}

impl LocalTable {
    pub fn try_new(db: Arc<OptiDBImpl>, tbl_name: String) -> Result<Self> {
        Ok(Self {
            db,
            tbl_name,
        })
    }
}

#[async_trait]
impl TableProvider for LocalTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(self.db.schema1(self.tbl_name.as_str()).unwrap())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let (schema, fields) = match projection {
            None => {
                let schema = self.schema().clone();
                let fields = self
                    .schema()
                    .fields
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect::<Vec<_>>();
                (schema, fields)
            }

            Some(idx) => {
                let schema = Arc::new(self.schema().project(idx)?);
                let fields = self
                    .schema()
                    .fields
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        if idx.contains(&i) {
                            Some(f.name().to_owned())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();

                (schema, fields)
            }
        };

        Ok(Arc::new(
            LocalExec::try_new(schema, self.db.clone(), "events".to_string(), fields)
                .map_err(|e| e.into_datafusion_execution_error())?,
        ) as Arc<dyn ExecutionPlan>)
    }
}
