use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use fnv::FnvHashMap;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::Result;

#[derive(Debug)]
pub struct PivotExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    /// reference to column with names
    name_col: Column,
    /// reference to column with values
    value_col: Column,
    /// value type
    value_type: DataType,
    group_cols: Vec<Column>,
    result_cols: Vec<String>,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

const BUFFER_LENGTH: usize = 1024;

impl PivotExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        name_col: Column,
        value_col: Column,
        result_cols: Vec<String>,
    ) -> Result<Self> {
        let group_cols: Vec<Column> = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| {
                match f.name() == name_col.name() || f.name() == value_col.name() {
                    true => None,
                    false => Some(Column::new(f.name(), idx)),
                }
            })
            .collect();

        let value_type = input.schema().field(value_col.index()).data_type().clone();
        let schema = {
            let group_fields: Vec<Field> = group_cols
                .iter()
                .map(|col| input.schema().field(col.index()).clone())
                .collect();
            let result_fields: Vec<Field> = result_cols
                .iter()
                .map(|col| Field::new(col, value_type.clone(), true))
                .collect();

            Arc::new(Schema::new([group_fields, result_fields].concat()))
        };

        let cache = Self::compute_properties(&input, schema.clone())?;
        Ok(Self {
            input,
            schema,
            name_col,
            value_col,
            value_type,
            group_cols,
            result_cols,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(), // Output Partitioning
            input.execution_mode(),              // Execution Mode
        ))
    }
}

impl DisplayAs for PivotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PivotExec")
    }
}

#[async_trait]
impl ExecutionPlan for PivotExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            PivotExec::try_new(
                children[0].clone(),
                self.name_col.clone(),
                self.value_col.clone(),
                self.result_cols.clone(),
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;

        let mut result_map: FnvHashMap<String, Vec<ScalarValue>> = FnvHashMap::default();
        for result_col in self.result_cols.clone() {
            result_map.insert(result_col, vec![
                ScalarValue::try_from(&self.value_type)?;
                BUFFER_LENGTH
            ]);
        }
        Ok(Box::pin(PivotStream {
            stream,
            schema: self.schema.clone(),
            group_cols: self.group_cols.clone(),
            result_cols: self.result_cols.clone(),
            name_col: self.name_col.clone(),
            value_col: self.value_col.clone(),
            value_type: self.value_type.clone(),
            group_map: FnvHashMap::default(),
            unique_groups: Vec::with_capacity(BUFFER_LENGTH),
            result_map,
            finished: false,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }
}

struct PivotStream {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    group_cols: Vec<Column>,
    result_cols: Vec<String>,
    name_col: Column,
    value_col: Column,
    value_type: DataType,
    group_map: FnvHashMap<u64, u32>,
    unique_groups: Vec<(u64, Vec<ScalarValue>)>,
    result_map: FnvHashMap<String, Vec<ScalarValue>>,
    finished: bool,
    baseline_metrics: BaselineMetrics,
}

impl PivotStream {
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<DFResult<RecordBatch>>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        loop {
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => self.pivot_batch(&batch)?,
                Poll::Ready(None) => {
                    self.finished = true;

                    // nothing was actually processed
                    if self.unique_groups.is_empty() {
                        return Poll::Ready(None);
                    }

                    return Poll::Ready(Some(Ok(self.finish()?)));
                }
                other => return other,
            }
        }
    }
}

impl RecordBatchStream for PivotStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl PivotStream {
    fn pivot_batch(&mut self, batch: &RecordBatch) -> ArrowResult<()> {
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut batch_hashes = vec![0; batch.num_rows()];

        let c = arrow::compute::cast(batch.column(self.name_col.index()), &DataType::Utf8)?;
        let name_arr = c
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| QueryError::Execution("name column cast to string error".to_string()))?;
        let value_arr = batch.column(self.value_col.index());

        let group_arrs: Vec<ArrayRef> = self
            .group_cols
            .iter()
            .map(|col| batch.column(col.index()).clone())
            .collect();
        create_hashes(&group_arrs, &random_state, &mut batch_hashes)?;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let group_idx = *self.group_map.entry(hash).or_insert_with(|| {
                let group_values = group_arrs
                    .iter()
                    .map(|arr| ScalarValue::try_from_array(arr, row).unwrap())
                    .collect();
                self.unique_groups.push((hash, group_values));
                self.unique_groups.len() as u32 - 1
            });

            let group_idx = group_idx as usize;
            let col_name = name_arr.value(row);

            match self.result_map.get_mut(col_name) {
                None => {
                    return Err(QueryError::Execution(format!(
                        "unknown name column \"{col_name:?}\""
                    ))
                    .into());
                }
                Some(values) => {
                    if values.len() - 1 < group_idx {
                        values.resize(
                            values.len() + BUFFER_LENGTH,
                            ScalarValue::try_from(&self.value_type)?,
                        );
                    }
                    values[group_idx] = if value_arr.is_null(row) {
                        ScalarValue::try_from(value_arr.data_type())?
                    } else {
                        ScalarValue::try_from_array(value_arr, row)?
                    };
                }
            }
        }

        Ok(())
    }

    fn finish(&mut self) -> ArrowResult<RecordBatch> {
        let group_arrs: Vec<ArrayRef> = self
            .group_cols
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let scalars: Vec<ScalarValue> = self
                    .unique_groups
                    .iter()
                    .map(|(_, values)| values[idx].to_owned())
                    .collect();
                ScalarValue::iter_to_array(scalars)
            })
            .collect::<DFResult<_>>()?;

        let result_arrs: Vec<ArrayRef> = self
            .result_cols
            .iter()
            .map(|col| {
                let unique_groups_len = self.unique_groups.len();
                let value_type = self.value_type.clone();
                let scalars = self.result_map.get_mut(col).unwrap();
                if scalars.len() != unique_groups_len {
                    scalars.resize(
                        unique_groups_len,
                        ScalarValue::try_from(&value_type).unwrap(),
                    );
                }

                ScalarValue::iter_to_array(scalars.clone())
            })
            .collect::<DFResult<_>>()?;

        let result_batch =
            RecordBatch::try_new(self.schema.clone(), [group_arrs, result_arrs].concat())?;

        Ok(result_batch)
    }
}

#[async_trait]
impl Stream for PivotStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::ArrayRef;
    use arrow::array::Float64Array;
    use arrow::array::Int32Array;
    use arrow::array::StringArray;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    pub use datafusion_common::Result;

    use crate::physical_plan::pivot::PivotExec;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    (
                        "d1",
                        Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()]))
                            as ArrayRef,
                    ),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                    ("n", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                    (
                        "v",
                        Arc::new(Float64Array::from(vec![4.3, 6.3])) as ArrayRef,
                    ),
                ])?,
                RecordBatch::try_from_iter(vec![
                    (
                        "d1",
                        Arc::new(StringArray::from(vec![
                            "a".to_string(),
                            "a".to_string(),
                            "c".to_string(),
                        ])) as ArrayRef,
                    ),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
                    ("n", Arc::new(Int32Array::from(vec![2, 1, 3])) as ArrayRef),
                    (
                        "v",
                        Arc::new(Float64Array::from(vec![1.0, 11.0, 23.3])) as ArrayRef,
                    ),
                ])?,
            ];

            let schema = batches[0].schema();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let exec = PivotExec::try_new(
            input.clone(),
            Column::new_with_schema("n", input.schema().as_ref())?,
            Column::new_with_schema("v", input.schema().as_ref())?,
            vec!["2".to_string(), "1".to_string(), "3".to_string()],
        )
        .unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = exec.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print!("{}", arrow::util::pretty::pretty_format_batches(&result)?);

        Ok(())
    }
}
