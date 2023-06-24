use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result as DFResult;
use futures::Stream;
use futures::StreamExt;

use crate::error::Result;
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

pub struct SegmentationExec {
    predicate: Arc<dyn SegmentationExpr>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
    out_batch_size: usize,
}

impl SegmentationExec {
    pub fn try_new(
        predicate: Arc<dyn SegmentationExpr>,
        partition_key: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
        out_batch_size: usize,
    ) -> Result<Self> {
        let res = Self {
            predicate,
            input,
            schema: input.schema(),
            metrics: Default::default(),
            partition_key,
            out_batch_size,
        };

        Ok(res)
    }
}
pub struct SegmentationStream {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Float32Array;
    use arrow::array::Int16Array;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use crate::physical_plan::expressions::segmentation;

    use crate::physical_plan::segmentation::SegmentationExec;

    #[tokio::test]
    async fn it_works() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int16, false),
        ]));
        let batches = {
            let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3]));
            let batch1 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            let col1: ArrayRef = Arc::new(Int64Array::from(vec![4, 1, 1, 2, 2, 2, 3, 3, 3]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3]));
            let batch1 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
            let col2: ArrayRef = Arc::new(Int16Array::from(vec![4, 5, 6, 1]));
            let batch2 =
                RecordBatch::try_new(schema.clone(), vec![col1.clone(), col2.clone()]).unwrap();

            vec![batch1, batch2]
        };

        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let expr = segmentation::aggregate::Aggregate::try_new()
        let segmentation = SegmentationExec::try_new()?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = funnel.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        println!("{}", pretty_format_batches(&result)?);
        Ok(())
    }
}
