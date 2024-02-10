use std::sync::Arc;

use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::tree_node::TreeNode;

pub struct ApplyParquetProjection {
    projection: Vec<usize>,
}

impl ApplyParquetProjection {
    pub fn new(projection: Vec<usize>) -> Self {
        Self { projection }
    }
}

impl PhysicalOptimizerRule for ApplyParquetProjection {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();
            if let Some(parquet) = plan_any.downcast_ref::<ParquetExec>() {
                let base_config = FileScanConfig {
                    projection: Some(self.projection.clone()),
                    ..parquet.base_config().clone()
                };
                let new_child = ParquetExec::new(base_config, parquet.predicate().cloned(), None);
                println!(">> @ {:?}", new_child.schema());

                return Ok(Transformed::Yes(Arc::new(new_child)));
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "apply_parquet_projection"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
