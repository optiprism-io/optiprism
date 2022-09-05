use crate::{Result};
use arrow::array::{Array, ArrayAccessor, ArrayRef, BooleanArray, BooleanBuilder, Decimal128Array, Decimal128Builder, Float32Builder, Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use axum::async_trait;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use datafusion::arrow::array::{
    Float32Array, Float64Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use datafusion::execution::context::TaskContext;
use crate::error::QueryError;
use datafusion_common::Result as DFResult;

/// `UNPIVOT` execution plan operator. Unpivot transforms columns into rows. E.g.
pub struct UnpivotExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cols: Vec<String>,
    name_col: String,
    value_col: String,
    metrics: ExecutionPlanMetricsSet,
}

impl UnpivotExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        cols: Vec<String>,
        name_col: String,
        value_col: String,
    ) -> Result<Self> {
        let value_type = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

        let mut uniq_cols = cols.clone();
        uniq_cols.sort();
        uniq_cols.dedup();
        if cols.len() != uniq_cols.len() {
            return Err(QueryError::Plan("non-unique column".to_string()));
        }

        for col in &cols {
            input.schema().index_of(col.as_str())?;
        }

        let schema = {
            let mut fields: Vec<Field> = input
                .schema()
                .fields()
                .iter()
                .filter_map(|f| match cols.contains(f.name()) {
                    true => None,
                    false => Some(f.clone()),
                })
                .collect();

            let name_field = Field::new(name_col.as_str(), DataType::Utf8, false);
            fields.push(name_field);
            let value_field = Field::new(value_col.as_str(), value_type, false);
            fields.push(value_field);

            Arc::new(Schema::new(fields))
        };

        Ok(Self {
            input,
            schema,
            cols,
            name_col,
            value_col,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for UnpivotExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UnpivotExec")
    }
}

#[async_trait]
impl ExecutionPlan for UnpivotExec {
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

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            UnpivotExec::try_new(
                children[0].clone(),
                self.cols.clone(),
                self.name_col.clone(),
                self.value_col.clone(),
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

        Ok(Box::pin(UnpivotStream {
            stream,
            schema: self.schema.clone(),
            cols: self.cols.clone(),
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnpivotExec")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct UnpivotStream {
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
    cols: Vec<String>,
    baseline_metrics: BaselineMetrics,
}

impl RecordBatchStream for UnpivotStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for UnpivotStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let poll = match self.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some(unpivot(&batch, self.schema.clone(), &self.cols)))
            }
            other => other,
        };

        self.baseline_metrics.record_poll(poll)
    }
}

macro_rules! build_group_arr {
    ($batch_col_idx:expr, $src_arr_ref:expr, $array_type:ident, $unpivot_cols_len:ident,$builder_type:ident) => {{
        // get typed source array
        let src_arr = $src_arr_ref.as_any().downcast_ref::<$array_type>().unwrap();
        // make result builder. The length of array is the lengs of source array multiplied by number of pivot columns
        let mut result = $builder_type::new($src_arr_ref.len()*$unpivot_cols_len);

        // populate the values from source array to result
        for row_idx in 0..$src_arr_ref.len() {
            if src_arr.is_null(row_idx) {
                    // append value multiple time, one for each unpivot column
                    for _ in 0..$unpivot_cols_len {
                        result.append_null()
                    }
                } else {
                // populate null
                for _ in 0..$unpivot_cols_len {
                        result.append_value(src_arr.value(row_idx));
                    }
                }
        }

        Ok(Arc::new(result.finish()) as ArrayRef)
    }};
}

macro_rules! build_value_arr {
    ($array_type:ident, $builder_type:ident, $builder_cap:expr, $unpivot_arrs:expr) => {{
        // get typed arrays
        let arrs: Vec<&$array_type> = $unpivot_arrs
            .iter()
            .map(|x| x.as_any().downcast_ref::<$array_type>().unwrap())
            .collect();
        // make result builder
        let mut result = $builder_type::new($builder_cap);

        // iterate over each row
        for idx in 0..$unpivot_arrs[0].len() {
            // iterate over each column to unpivot and append its value to the result
            for arr in arrs.iter() {
                if arr.is_null(idx) {
                    result.append_null();
                } else {
                    result.append_value(arr.value(idx));
                }
            }
        }

        Arc::new(result.finish()) as ArrayRef
    }};
}

pub fn unpivot(
    batch: &RecordBatch,
    schema: SchemaRef,
    cols: &[String],
) -> ArrowResult<RecordBatch> {
    let builder_cap = batch.num_rows() * cols.len();
    let unpivot_cols_len = cols.len();

    let group_arrs: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| !cols.contains(batch.schema().field(*idx).name()))
        .map(|(_, arr)| match arr.data_type() {
            DataType::Int8 => {
                build_group_arr!(batch_col_idx, arr, Int8Array, unpivot_cols_len, Int8Builder)
            }
            DataType::Int16 => build_group_arr!(
                batch_col_idx,
                arr,
                Int16Array,
                unpivot_cols_len,
                Int16Builder
            ),
            DataType::Int32 => build_group_arr!(
                batch_col_idx,
                arr,
                Int32Array,
                unpivot_cols_len,
                Int32Builder
            ),
            DataType::Int64 => build_group_arr!(
                batch_col_idx,
                arr,
                Int64Array,
                unpivot_cols_len,
                Int64Builder
            ),
            DataType::UInt8 => build_group_arr!(
                batch_col_idx,
                arr,
                UInt8Array,
                unpivot_cols_len,
                UInt8Builder
            ),
            DataType::UInt16 => build_group_arr!(
                batch_col_idx,
                arr,
                UInt16Array,
                unpivot_cols_len,
                UInt16Builder
            ),
            DataType::UInt32 => build_group_arr!(
                batch_col_idx,
                arr,
                UInt32Array,
                unpivot_cols_len,
                UInt32Builder
            ),
            DataType::UInt64 => build_group_arr!(
                batch_col_idx,
                arr,
                UInt64Array,
                unpivot_cols_len,
                UInt64Builder
            ),
            DataType::Boolean => build_group_arr!(
                batch_col_idx,
                arr,
                BooleanArray,
                unpivot_cols_len,
                BooleanBuilder
            ),
            DataType::Float32 => build_group_arr!(
                batch_col_idx,
                arr,
                Float32Array,
                unpivot_cols_len,
                Float32Builder
            ),
            DataType::Float64 => build_group_arr!(
                batch_col_idx,
                arr,
                Float64Array,
                unpivot_cols_len,
                Float64Builder
            ),
            DataType::Utf8 => build_group_arr!(
                batch_col_idx,
                arr,
                StringArray,
                unpivot_cols_len,
                StringBuilder
            ),
            DataType::Timestamp(_, None) => build_group_arr!(
                batch_col_idx,
                arr,
                TimestampNanosecondArray,
                unpivot_cols_len,
                TimestampNanosecondBuilder
            ),
            DataType::Decimal128(precision, scale) => {
                // build group array realisation for decimal type
                let src_arr_typed = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let mut result = Decimal128Builder::new(builder_cap, *precision, *scale);

                for row_idx in 0..arr.len() {
                    if src_arr_typed.is_null(row_idx) {
                        for _ in 0..=unpivot_cols_len {
                            result.append_null();
                        }
                    } else {
                        for _ in 0..=unpivot_cols_len {
                            if let Err(err) = result.append_value(src_arr_typed.value(row_idx)) {
                                return Err(err);
                            }
                        }
                    }
                }

                Ok(Arc::new(result.finish()) as ArrayRef)
            }
            _ => unimplemented!("{}", arr.data_type()),
        })
        .collect::<ArrowResult<Vec<_>>>()?;

    // define value type
    let value_type = DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE);

    // cast unpivot cols to value type
    let unpivot_arrs: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _)| cols.contains(batch.schema().field(*idx).name()))
        .map(|(_, arr)| match arr.data_type() {
            DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE) => arr.clone(),
            DataType::UInt64 => {
                // first cast uint to int because Arrow 9.1.0 doesn't support casting from uint to decimal
                let int_arr = arrow::compute::cast(arr, &DataType::Int64).unwrap();
                arrow::compute::cast(&int_arr, &value_type).unwrap()
            }
            _other => arrow::compute::cast(arr, &value_type).unwrap(),
        })
        .collect();

    let name_arr = {
        let mut builder = StringBuilder::new(builder_cap);
        for _ in 0..batch.num_rows() {
            for c in cols.iter() {
                builder.append_value(c.as_str());
            }
        }

        Arc::new(builder.finish()) as ArrayRef
    };

    let value_arr: ArrayRef = match value_type {
        DataType::Int8 => build_value_arr!(Int8Array, Int8Builder, builder_cap, unpivot_arrs),
        DataType::Int16 => build_value_arr!(Int16Array, Int16Builder, builder_cap, unpivot_arrs),
        DataType::UInt64 => build_value_arr!(UInt64Array, UInt64Builder, builder_cap, unpivot_arrs),
        DataType::Float64 => {
            build_value_arr!(Float64Array, Float64Builder, builder_cap, unpivot_arrs)
        }
        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE) => {
            let arrs: Vec<&Decimal128Array> = unpivot_arrs
                .iter()
                .map(|x| x.as_any().downcast_ref::<Decimal128Array>().unwrap())
                .collect();
            let mut result = Decimal128Builder::new(builder_cap, DECIMAL_PRECISION, DECIMAL_SCALE);

            for idx in 0..unpivot_arrs[0].len() {
                for arr in arrs.iter() {
                    if arr.is_null(idx) {
                        result.append_null();
                    } else {
                        result.append_value(arr.value(idx))?;
                    }
                }
            }

            Arc::new(result.finish()) as ArrayRef
        }

        _ => unimplemented!("{}", value_type),
    };

    let mut final_arrs = group_arrs.clone();
    final_arrs.push(name_arr);
    final_arrs.push(value_arr);

    RecordBatch::try_new(schema, final_arrs)
}

#[cfg(test)]
mod tests {
    use crate::physical_plan::unpivot::UnpivotExec;
    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;
    pub use datafusion_common::Result;

    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use std::sync::Arc;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test() -> Result<()> {
        let input = {
            let batches = vec![
                RecordBatch::try_from_iter(vec![
                    (
                        "d1",
                        Arc::new(StringArray::from(vec!["a".to_string(), "b".to_string()]))
                            as ArrayRef,
                    ),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
                    (
                        "v1",
                        Arc::new(Float64Array::from(vec![1.34, 2.0])) as ArrayRef,
                    ),
                    (
                        "v2",
                        Arc::new(Float32Array::from(vec![4.3, 6.3])) as ArrayRef,
                    ),
                ])?,
                RecordBatch::try_from_iter(vec![
                    (
                        "d1",
                        Arc::new(StringArray::from(vec![
                            "a".to_string(),
                            "b".to_string(),
                            "c".to_string(),
                        ])) as ArrayRef,
                    ),
                    ("d2", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
                    (
                        "v1",
                        Arc::new(Float64Array::from(vec![2.72, 23.0, 33.3])) as ArrayRef,
                    ),
                    (
                        "v2",
                        Arc::new(Float32Array::from(vec![1.0, 11.0, 2.23])) as ArrayRef,
                    ),
                ])?,
            ];

            let schema = batches[0].schema();
            Arc::new(MemoryExec::try_new(&[batches], schema, None).unwrap())
        };

        let exec = UnpivotExec::try_new(
            input,
            vec!["v1".to_string(), "v2".to_string()],
            "name".to_string(),
            "value".to_string(),
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
