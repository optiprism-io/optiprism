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
use store::db::OptiDB;
use store::db::OptiDBImpl;
use store::db::ScanStream;
use store::error::StoreError;

use crate::error::QueryError;
use crate::error::Result;

#[derive(Debug)]
pub struct LocalExec {
    schema: SchemaRef,
    streams: Mutex<Vec<Option<ScanStream<File>>>>,
}

impl LocalExec {
    pub fn try_new(schema: SchemaRef, streams: Vec<ScanStream<File>>) -> Result<Self> {
        Ok(Self {
            schema,
            streams: Mutex::new(streams.into_iter().map(|s| Some(s)).collect()),
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
        Partitioning::UnknownPartitioning(self.streams.lock().unwrap().len())
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
            streams: Mutex::new(self.streams.lock().unwrap().clone()),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let mut streams = self.streams.lock().unwrap();
        let stream = mem::replace(&mut streams[partition], None);
        Ok(Box::pin(PartitionStream {
            local_stream: Box::pin(stream.unwrap()),
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
    use store::db::TableOptions;
    use store::KeyValue;
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
            partition_keys: vec![0],
            l1_max_size_bytes: 1024 * 1024 * 10,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024 * 1024 * 10,
            merge_array_page_size: 10000,
            merge_data_page_size_limit_bytes: Some(1024 * 1024),
            merge_index_cols: 2,
            merge_max_part_size_bytes: 2048,
            merge_row_group_values_limit: 1000,
            read_chunk_size: 10,
            merge_array_size: 100,
        };
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        db.add_field(Field::new("a", DataType::Int64, false))
            .unwrap();
        db.add_field(Field::new("b", DataType::Int64, false))
            .unwrap();
        db.add_field(Field::new("c", DataType::Int64, false))
            .unwrap();

        for i in 0..1000 {
            db.insert(vec![KeyValue::Int64(i), KeyValue::Int64(i)], vec![
                Value::Int64(Some(i)),
            ])
            .unwrap();
        }

        let schema = schema2_to_schema1(db.schema()?);
        let prov = LocalTable::try_new(Arc::new(db), Arc::new(schema), 1).unwrap();
        let table_source = Arc::new(DefaultTableSource::new(
            Arc::new(prov) as Arc<dyn TableProvider>
        ));
        let input = datafusion_expr::LogicalPlanBuilder::scan("table", table_source, None)
            .unwrap()
            .build()
            .unwrap();

        let runtime = Arc::new(RuntimeEnv::default());
        let state =
            SessionState::with_config_rt(SessionConfig::new().with_target_partitions(1), runtime)
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
