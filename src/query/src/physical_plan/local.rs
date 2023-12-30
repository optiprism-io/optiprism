use std::any::Any;
use std::fs::File;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::Statistics;
use futures::Stream;
use futures::StreamExt;
use futures::TryStream;
use store::arrow_conversion::arrow2_to_arrow1;
use store::db::OptiDBImpl;
use store::db::ScanStream;
use store::error::StoreError;
use tracing::debug;
use tracing::info;
use tracing::trace;

use crate::error::QueryError;
use crate::error::Result;

#[derive(Debug)]
pub struct LocalExec {
    schema: SchemaRef,
    // streams: Arc<Mutex<Vec<Option<ScanStream>>>>,
    db: Arc<OptiDBImpl>,
    tbl_name: String,
    fields: Vec<String>,
}

impl LocalExec {
    pub fn try_new(
        schema: SchemaRef,
        db: Arc<OptiDBImpl>,
        tbl_name: String,
        fields: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            db,
            tbl_name,
            fields,
        })
    }
}

struct PartitionStream {
    local_stream: Pin<Box<dyn Stream<Item = store::error::Result<Chunk<Box<dyn Array>>>> + Send>>,
    schema: SchemaRef,
}

impl RecordBatchStream for PartitionStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for PartitionStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let v = match self.local_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                let arrs = chunk
                    .into_arrays()
                    .into_iter()
                    .map(|arr| arrow2_to_arrow1::convert(arr))
                    .collect::<store::error::Result<Vec<_>>>()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                let vv = RecordBatch::try_new(self.schema.clone(), arrs.clone());
                Poll::Ready(Some(Ok(RecordBatch::try_new(self.schema.clone(), arrs)?)))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DataFusionError::Execution(e.to_string()))))
            }
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
        };

        v
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
        Partitioning::UnknownPartitioning(
            self.db
                .table_options(self.tbl_name.as_str())
                .unwrap()
                .partitions,
        )
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LocalExec {
            schema: self.schema.clone(),
            db: self.db.clone(),
            tbl_name: self.tbl_name.clone(),
            fields: self.fields.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _cx: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        debug!("pp {partition}");
        let stream = self
            .db
            .scan_partition(self.tbl_name.as_str(), partition, self.fields.clone())
            .map_err(|e| {
                DataFusionError::Execution(format!("Error executing local plan: {:?}", e))
            })?;

        Ok(Box::pin(PartitionStream {
            local_stream: Box::pin(stream),
            schema: self.schema.clone(),
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use common::types::DType;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::context::SessionState;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use store::arrow_conversion::schema2_to_schema1;
    use store::db::OptiDBImpl;
    use store::db::Options;
    use store::db::TableOptions;
    use store::KeyValue;
    use store::NamedValue;
    use store::Value;
    use tracing::debug;

    use crate::datasources::local::LocalTable;
    use crate::physical_plan::planner::planner::QueryPlanner;

    #[tokio::test]
    async fn test() {
        let path = PathBuf::from("/opt/homebrew/Caskroom/clickhouse/user_files");
        fs::remove_dir_all(&path).unwrap();
        // fs::create_dir_all(&path).unwrap();

        let opts = TableOptions {
            partitions: 2,
            index_cols: 2,
            l1_max_size_bytes: 1024 * 1024 * 10,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024 * 1024 * 10,
            merge_array_page_size: 10000,
            merge_data_page_size_limit_bytes: Some(1024 * 1024),
            merge_index_cols: 2,
            merge_max_l1_part_size_bytes: 2048,
            merge_row_group_values_limit: 1000,
            merge_array_size: 100,
            levels: 7,
            merge_part_size_multiplier: 0,
            merge_chunk_size: 1024 * 8 * 8,
        };
        let mut db = OptiDBImpl::open(path, Options {}).unwrap();
        db.create_table("events", opts).unwrap();
        db.add_field("events", "a", DType::Int64, false).unwrap();
        db.add_field("events", "b", DType::Int64, false).unwrap();
        db.add_field("events", "c", DType::Int64, false).unwrap();

        let a = NamedValue::new("a".to_string(), Value::Int64(None));
        for i in 0..1000 {
            db.insert("events", vec![
                NamedValue::new("a".to_string(), Value::Int64(Some(i))),
                NamedValue::new("b".to_string(), Value::Int64(Some(i))),
                NamedValue::new("c".to_string(), Value::Int64(Some(i))),
            ])
            .unwrap();
        }

        let schema = schema2_to_schema1(db.schema("events").unwrap());
        let prov = LocalTable::try_new(Arc::new(db), "events".to_string()).unwrap();
        let table_source = Arc::new(DefaultTableSource::new(
            Arc::new(prov) as Arc<dyn TableProvider>
        ));
        let input = datafusion_expr::LogicalPlanBuilder::scan("table", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        let runtime = Arc::new(RuntimeEnv::default());
        let state =
            SessionState::with_config_rt(SessionConfig::new().with_target_partitions(12), runtime)
                .with_query_planner(Arc::new(QueryPlanner {}))
                .with_optimizer_rules(vec![]);
        let exec_ctx = SessionContext::with_state(state.clone());
        let physical_plan = state.create_physical_plan(&input).await.unwrap();
        let displayable_plan = displayable(physical_plan.as_ref());

        debug!("physical plan: {}", displayable_plan.indent(true));
        let batches = collect(physical_plan, exec_ctx.task_ctx()).await.unwrap();
        print_batches(batches.as_ref()).unwrap();
    }
}
