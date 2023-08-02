use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::UInt64Array;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;
use crate::Result;

pub struct SegmentedAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    partition_inputs: Vec<Arc<dyn ExecutionPlan>>,
    partition_col: Column,
    agg_expr: Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>,
    agg_aliases: Vec<String>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
}

impl SegmentedAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_inputs: Vec<Arc<dyn ExecutionPlan>>,
        partition_col: Column,
        agg_expr: Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>,
        agg_aliases: Vec<String>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut agg_schemas: Vec<SchemaRef> = Vec::new();
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<FieldRef> = Vec::new();
        for (agg_idx, agg) in agg_expr.iter().enumerate() {
            let mut agg_fields: Vec<FieldRef> = Vec::new();
            let agg = agg.lock().unwrap();
            for group_col in agg.group_columns() {
                group_cols.insert(group_col.name().to_string(), ());
                agg_fields.push(input_schema.fields[group_col.index()].clone());
            }

            for f in agg.fields().iter() {
                let f = Field::new(
                    format!("{}_{}", agg_aliases[agg_idx], f.name()),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                );
                agg_result_fields.push(f.clone().into());
                agg_fields.push(f.into());
            }

            agg_schemas.push(Arc::new(Schema::new(agg_fields)));
        }

        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false)) as FieldRef;
        let group_fields = input_schema
            .fields
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let fields: Vec<FieldRef> =
            vec![vec![segment_field], group_fields, agg_result_fields].concat();

        let schema = Schema::new(fields);
        Ok(Self {
            input,
            partition_inputs,
            partition_col,
            agg_expr,
            agg_aliases,
            schema: Arc::new(schema),
            agg_schemas,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for SegmentedAggregateExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentedAggregateExec")
    }
}

#[async_trait]
impl ExecutionPlan for SegmentedAggregateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentedAggregateExec::try_new(
                children[0].clone(),
                self.partition_inputs.clone(),
                self.partition_col.clone(),
                self.agg_expr.clone(),
                self.agg_aliases.clone(),
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context.clone())?;
        let partition_streams = self
            .partition_inputs
            .iter()
            .map(|s| s.execute(partition, context.clone()))
            .collect::<DFResult<Vec<_>>>()?;
        let _baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let agg_expr = (0..self.partition_inputs.len())
            .into_iter()
            .map(|_| {
                self.agg_expr
                    .iter()
                    .map(|a| {
                        let mut agg = a.lock().unwrap();
                        Arc::new(Mutex::new(agg.make_new().unwrap()))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        Ok(Box::pin(SegmentedAggregateStream {
            is_ended: false,
            stream,
            schema: self.schema.clone(),
            agg_schemas: self.agg_schemas.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
            partition_col: self.partition_col.clone(),
            partition_streams,
            agg_expr,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentedAggregateExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct SegmentedAggregateStream {
    is_ended: bool,
    stream: SendableRecordBatchStream,
    partition_streams: Vec<SendableRecordBatchStream>,
    partition_col: Column,
    agg_expr: Vec<Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for SegmentedAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for SegmentedAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }
        self.is_ended = true;
        let mut exist: Vec<HashMap<i64, ()>> = vec![HashMap::new(); self.partition_streams.len()];

        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let segments_count = self.partition_streams.len();
        let partition_col = self.partition_col.clone();

        for (segment_id, partitioned_stream) in self.partition_streams.iter_mut().enumerate() {
            loop {
                match partitioned_stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        let vals = partition_col
                            .evaluate(&batch)?
                            .into_array(batch.num_rows())
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .clone();

                        for val in vals.iter() {
                            exist[segment_id].insert(val.unwrap(), ());
                        }
                    }

                    Poll::Ready(None) => {
                        break;
                    }
                    _ => unreachable!(),
                }
            }
        }

        loop {
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    for segment in 0..segments_count {
                        for aggm in self.agg_expr[segment].iter() {
                            let mut agg = aggm.lock().unwrap();
                            agg.evaluate(&batch, &exist[segment])
                                .map_err(QueryError::into_datafusion_execution_error)?
                        }
                    }
                }
                Poll::Ready(None) => {
                    break;
                }
                other => return other,
            };
        }

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(10);
        for segment in 0..segments_count {
            for (agg_idx, aggrm) in self.agg_expr[segment].iter().enumerate() {
                let mut aggr = aggrm.lock().unwrap();
                let agg_cols = aggr
                    .finalize()
                    .map_err(QueryError::into_datafusion_execution_error)?;
                let seg_col =
                    ScalarValue::Int64(Some(segment as i64)).to_array_of_size(agg_cols[0].len());
                let cols = vec![vec![seg_col], agg_cols].concat();
                let schema = self.agg_schemas[agg_idx].clone();
                let schema = Arc::new(Schema::new(
                    vec![
                        vec![Arc::new(Field::new("segment", DataType::Int64, false))],
                        schema.fields().to_vec(),
                    ]
                    .concat(),
                ));
                let batch = RecordBatch::try_new(schema, cols)?;
                let cols = self
                    .schema
                    .fields()
                    .iter()
                    .map(
                        |field| match batch.schema().index_of(field.name().as_str()) {
                            Ok(col_idx) => Ok(batch.column(col_idx).clone()),
                            Err(_) => {
                                let v = ScalarValue::try_from(field.data_type())?;
                                Ok(v.to_array_of_size(batch.column(0).len()))
                            }
                        },
                    )
                    .collect::<DFResult<Vec<ArrayRef>>>()?;

                let result = RecordBatch::try_new(self.schema.clone(), cols)?;
                batches.push(result);
            }
        }

        let batch = concat_batches(&self.schema, batches.iter().map(|b| b).collect::<Vec<_>>())?;

        Poll::Ready(Some(Ok(batch)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Int32Array;
    use arrow::array::Int8Array;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use arrow_row::SortField;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    pub use datafusion_common::Result;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::count;
    use crate::physical_plan::expressions::partitioned2::count_grouped::Count;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;
    use crate::physical_plan::segmented_aggregate::SegmentedAggregateExec;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | country(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|---------------|--------|--------|-------------|
| 0            | iphone       | Spain         | 1      | 1      | e1          |
| 0            | iphone       | Spain         | 0      | 2      | e2          |
| 0            | iphone       | Spain         | 0      | 3      | e3          |
| 0            | android      | Spain         | 1      | 4      | e1          |
| 0            | android      | Spain         | 1      | 5      | e2          |
|||||||
| 0            | android      | Spain         | 0      | 6      | e3          |
| 1            | osx          | Germany       | 1      | 1      | e1          |
| 1            | osx          | Germany       | 1      | 2      | e2          |
| 1            | osx          | Germany       | 0      | 3      | e3          |
| 1            | osx          | UK            | 0      | 4      | e1          |
| 1            | osx          | UK            | 0      | 5      | e2          |
|||||||
| 1            | osx          | UK            | 0      | 6      | e3          |
| 2            | osx          | Portugal      | 1      | 1      | e1          |
| 2            | osx          | Portugal      | 1      | 2      | e2          |
| 2            | osx          | Portugal      | 0      | 3      | e3          |
| 2            | osx          | Spain         | 0      | 4      | e1          |
| 2            | osx          | Spain         | 0      | 5      | e2          |
| 2            | osx          | Spain         | 0      | 6      | e3          |
| 3            | osx          | Spain         | 1      | 1      | e1          |
| 3            | osx          | Spain         | 1      | 2      | e2          |
|||||||
| 3            | osx          | Spain         | 0      | 3      | e3          |
| 3            | osx          | UK            | 0      | 4      | e1          |
| 3            | osx          | UK            | 0      | 5      | e2          |
| 3            | osx          | UK            | 0      | 6      | e3          |
| 4            | osx          | Russia        | 0      | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let agg1 = {
            let groups = vec![SortField::new(DataType::Utf8)];
            let group_cols = vec![Column::new_with_schema("device", &schema).unwrap()];
            let count = Count::try_new(
                None,
                AggregateFunction::new_avg(),
                groups,
                group_cols,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let agg2 = {
            let groups = vec![
                SortField::new(DataType::Utf8),
                // SortField::new(DataType::Utf8),
            ];
            let group_cols = vec![
                Column::new_with_schema("country", &schema).unwrap(),
                // Column::new_with_schema("device", &schema).unwrap(),
            ];
            let count = Count::try_new(
                None,
                AggregateFunction::new_sum(),
                groups,
                group_cols,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pinput1 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
||
| 3            |
| 4            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

            input
        };

        let pinput2 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
| 1            |
| 2            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

            input
        };

        let seg = SegmentedAggregateExec::try_new(
            Arc::new(input),
            vec![Arc::new(pinput2), Arc::new(pinput1)],
            Column::new_with_schema("user_id", &schema).unwrap(),
            vec![agg1, agg2],
            vec!["count".to_string(), "min".to_string()],
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_ungrouped() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | country(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|---------------|--------|--------|-------------|
| 0            | iphone       | Spain         | 1      | 1      | e1          |
| 0            | iphone       | Spain         | 0      | 2      | e2          |
| 0            | iphone       | Spain         | 0      | 3      | e3          |
| 0            | android      | Spain         | 1      | 4      | e1          |
| 0            | android      | Spain         | 1      | 5      | e2          |
|||||||
| 0            | android      | Spain         | 0      | 6      | e3          |
| 1            | osx          | Germany       | 1      | 1      | e1          |
| 1            | osx          | Germany       | 1      | 2      | e2          |
| 1            | osx          | Germany       | 0      | 3      | e3          |
| 1            | osx          | UK            | 0      | 4      | e1          |
| 1            | osx          | UK            | 0      | 5      | e2          |
|||||||
| 1            | osx          | UK            | 0      | 6      | e3          |
| 2            | osx          | Portugal      | 1      | 1      | e1          |
| 2            | osx          | Portugal      | 1      | 2      | e2          |
| 2            | osx          | Portugal      | 0      | 3      | e3          |
| 2            | osx          | Spain         | 0      | 4      | e1          |
| 2            | osx          | Spain         | 0      | 5      | e2          |
| 2            | osx          | Spain         | 0      | 6      | e3          |
| 3            | osx          | Spain         | 1      | 1      | e1          |
| 3            | osx          | Spain         | 1      | 2      | e2          |
|||||||
| 3            | osx          | Spain         | 0      | 3      | e3          |
| 3            | osx          | UK            | 0      | 4      | e1          |
| 3            | osx          | UK            | 0      | 5      | e2          |
| 3            | osx          | UK            | 0      | 6      | e3          |
| 4            | osx          | Russia        | 0      | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

        let agg1 = {
            let count = count::Count::try_new(
                None,
                AggregateFunction::new_avg(),
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let agg2 = {
            let groups = vec![
                SortField::new(DataType::Utf8),
                // SortField::new(DataType::Utf8),
            ];
            let group_cols = vec![
                Column::new_with_schema("country", &schema).unwrap(),
                // Column::new_with_schema("device", &schema).unwrap(),
            ];
            let count = Count::try_new(
                None,
                AggregateFunction::new_sum(),
                groups,
                group_cols,
                Column::new_with_schema("user_id", &schema).unwrap(),
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pinput1 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
||
| 3            |
| 4            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

            input
        };

        let pinput2 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
| 1            |
| 2            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            let input = MemoryExec::try_new(&vec![batches], schema.clone(), None)?;

            input
        };

        let seg = SegmentedAggregateExec::try_new(
            Arc::new(input),
            vec![Arc::new(pinput2), Arc::new(pinput1)],
            Column::new_with_schema("user_id", &schema).unwrap(),
            vec![agg1 /* ,  agg2 */],
            vec!["count".to_string(), "min".to_string()],
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }
}
