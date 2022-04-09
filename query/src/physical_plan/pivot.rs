use std::any::Any;
use std::borrow::Borrow;
use std::collections::hash_map::Entry;
use ahash::RandomState;
use std::pin::Pin;
use futures::{Stream, StreamExt};
use std::sync::Arc;
use std::task::{Context, Poll};
use arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use fnv::FnvHashMap;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::expressions::{Column, PhysicalSortExpr};
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion_common::ScalarValue;
use crate::{Result, Error};
use arrow::error::{ArrowError, Result as ArrowResult};
use datafusion::error::Result as DFResult;
use axum::{async_trait};

#[derive(Debug)]
pub struct PivotExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    name_col: Column,
    value_col: Column,
    value_type: DataType,
    group_cols: Vec<Column>,
    result_cols: Vec<String>,
}

const BUFFER_LENGTH: usize = 2 ^ 10;

impl PivotExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, name_col: Column, value_col: Column, result_cols: Vec<String>) -> Result<Self> {
        let group_cols: Vec<Column> = input.schema().fields().iter().enumerate().filter_map(|(idx, f)| {
            match f.name() == name_col.name() || f.name() == value_col.name() {
                true => None,
                false => Some(Column::new(f.name(), idx))
            }
        }).collect();

        let value_type = input.schema().field(value_col.index()).data_type().clone();
        let schema = {
            let group_fields: Vec<Field> = group_cols
                .iter()
                .map(|col| input.schema().field(col.index()).clone())
                .collect();
            let result_fields: Vec<Field> = result_cols.iter().map(|col| Field::new(col, value_type.clone(), true)).collect();

            Arc::new(Schema::new([group_fields, result_fields].concat()))
        };

        Ok(Self {
            input,
            schema,
            name_col,
            value_col,
            value_type,
            group_cols,
            result_cols,
        })
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

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PivotExec::try_new(
            children[0].clone(),
            self.name_col.clone(),
            self.value_col.clone(),
            self.result_cols.clone(),
        ).map_err(Error::into_datafusion_execution_error)?))
    }

    async fn execute(&self, partition: usize, runtime: Arc<RuntimeEnv>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, runtime.clone()).await?;

        let mut result_map: FnvHashMap<String, Vec<ScalarValue>> = FnvHashMap::default();
        for result_col in self.result_cols.clone() {
            result_map.insert(result_col, vec![ScalarValue::try_from(&self.value_type)?; BUFFER_LENGTH]);
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
        }))
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
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
}

impl RecordBatchStream for PivotStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl PivotStream {
    fn pivot_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        println!("pivot");
        let random_state = RandomState::with_seeds(0, 0, 0, 0);
        let mut batch_hashes = vec![0; batch.num_rows()];

        let c = arrow::compute::cast(
            batch.column(self.name_col.index()),
            &DataType::Utf8,
        )?;

        let name_arr = c.as_any().downcast_ref::<StringArray>().ok_or_else(|| Error::QueryError("name column cast to string error".to_string()))?;
        let value_arr = batch.column(self.value_col.index()).as_any().downcast_ref::<Float64Array>().unwrap();

        let group_arrs: Vec<ArrayRef> = self.group_cols.iter().map(|col| batch.column(col.index()).clone()).collect();
        create_hashes(&group_arrs, &random_state, &mut batch_hashes)?;

        for (row, hash) in batch_hashes.into_iter().enumerate() {
            let group_idx = *self.group_map.entry(hash).or_insert_with(|| {
                println!("hash {} {}", hash, self.unique_groups.len());
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
                None => return Err(Error::QueryError("unknown name column".to_string())),
                Some(values) => {
                    println!("{} {} sd", values.len(), group_idx);
                    if values.len() - 1 < group_idx {
                        values.resize(values.len() + BUFFER_LENGTH, ScalarValue::try_from(&self.value_type)?);
                    }
                    values[group_idx] = if value_arr.is_null(row) {
                        ScalarValue::Float64(None)
                    } else {
                        println!("{} {}", value_arr.len(), group_idx);
                        ScalarValue::Float64(Some(value_arr.value(row)))
                    };
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Stream for PivotStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    match self.pivot_batch(&batch) {
                        Ok(_) => {}
                        Err(err) => return Poll::Ready(Some(Err(ArrowError::ExternalError(Box::new(err)))))
                    }
                }
                Poll::Ready(None) => {
                    self.finished = true;

                    let group_arrs: Vec<ArrayRef> = self.group_cols.iter().enumerate().map(|(idx, _)| {
                        let scalars: Vec<ScalarValue> = self.unique_groups.iter().map(|(_, values)| values[idx].clone()).collect();
                        ScalarValue::iter_to_array(scalars).unwrap()
                    }).collect();

                    let result_arrs: Vec<ArrayRef> = self.result_cols.clone().iter().map(|col| {
                        let unique_groups_len = self.unique_groups.len();
                        let value_type = self.value_type.clone();
                        let scalars = self.result_map.get_mut(col).unwrap();
                        if scalars.len() > unique_groups_len {
                            scalars.resize(unique_groups_len, ScalarValue::try_from(&value_type).unwrap())
                        }

                        // TODO remove clone
                        ScalarValue::iter_to_array(scalars.clone()).unwrap()
                    }).collect();

                    let result_batch = RecordBatch::try_new(
                        self.schema.clone(),
                        [group_arrs, result_arrs].concat(),
                    );

                    return Poll::Ready(Some(result_batch));
                }
                other => return other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use crate::physical_plan::pivot::PivotExec;
    use datafusion::physical_plan::ExecutionPlan;
    pub use datafusion::error::Result;
    use datafusion::physical_plan::expressions::Column;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    ("d1", Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string()])) as ArrayRef),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                    ("n", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                    ("v", Arc::new(Float64Array::from(vec![4.3, 6.3])) as ArrayRef),
                ])?,
                RecordBatch::try_from_iter(vec![
                    ("d1", Arc::new(StringArray::from(vec!["a".to_string(), "a".to_string(), "c".to_string()])) as ArrayRef),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
                    ("n", Arc::new(Int32Array::from(vec![2, 1, 3])) as ArrayRef),
                    ("v", Arc::new(Float64Array::from(vec![1.0, 11.0, 23.3])) as ArrayRef),
                ])?,
            ];

            let schema = batches[0].schema().clone();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let exec = PivotExec::try_new(
            input.clone(),
            Column::new_with_schema("n", input.schema().as_ref())?,
            Column::new_with_schema("v", input.schema().as_ref())?,
            vec!["2".to_string(), "1".to_string(), "3".to_string()],
        ).unwrap();
        let runtime = Arc::new(RuntimeEnv::new(RuntimeConfig::new())?);
        let stream = exec.execute(0, runtime).await?;
        let result = collect(stream).await?;

        print!("{}", arrow::util::pretty::pretty_format_batches(&result)?);

        Ok(())
    }
}