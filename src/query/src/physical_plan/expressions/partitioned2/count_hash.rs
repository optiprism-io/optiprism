use std::collections::HashMap;
use std::sync::Mutex;

use ahash::AHasher;
use ahash::RandomState;
use arrow::array::BooleanArray;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use arrow::row::Row;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::partitioned2::AggregateFunction;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

struct CountInner {
    count: i64,
    outer_fn: AggregateFunction,
    first: bool,
    last_partition: i64,
}

impl CountInner {
    pub fn new(outer_fn: AggregateFunction) -> Self {
        Self {
            count: 0,
            outer_fn,
            first: false,
            last_partition: 0,
        }
    }
}

pub struct Count {
    filter: Option<PhysicalExprRef>,
    outer_fn: AggregateFunction,
    group: Vec<Column>,
    inner: Mutex<HashMap<OwnedRow, CountInner, RandomState>>,
}

impl Count {
    pub fn new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction,
        group: Vec<Column>,
    ) -> Self {
        Self {
            filter,
            outer_fn,
            group,
            inner: Mutex::new(HashMap::default()),
        }
    }
}

impl PartitionedAggregateExpr for Count {
    fn evaluate(&self, batch: &RecordBatch, partitions: &ScalarBuffer<i64>) -> crate::Result<()> {
        let filter = if self.filter.is_some() {
            Some(
                self.filter
                    .clone()
                    .unwrap()
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .clone(),
            )
        } else {
            None
        };

        let rows = convert_columns()
        let mut inner = self.inner.lock().unwrap();

        for (row_id, partition) in partitions.into_iter().enumerate() {
            if inner.first {
                inner.first = false;
                inner.last_partition = *partition;
            }

            if let Some(filter) = &filter {
                if !check_filter(filter, row_id) {
                    continue;
                }
            }

            if inner.last_partition != *partition {
                let v = inner.count as i128;
                inner.outer_fn.accumulate(v);
                inner.last_partition = *partition;

                inner.count = 0;
            }
            inner.count += 1;
        }

        Ok(())
    }

    fn data_types(&self) -> Vec<DataType> {
        vec![DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)]
    }

    fn finalize(&self) -> Vec<ColumnarValue> {
        let mut inner = self.inner.lock().unwrap();
        let v = inner.count as i128;
        inner.outer_fn.accumulate(v);
        let result = inner.outer_fn.result();
        let v = ColumnarValue::Scalar(ScalarValue::Decimal128(
            Some(result),
            DECIMAL_PRECISION,
            DECIMAL_SCALE,
        ));

        vec![v]
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::count2::Count;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

    #[test]
    fn count_sum() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | 1          |
| 0            | 2      | 1          |
|              |        |             |
| 1            | 8      | 1          |
|              |        |             |
| 1            | 1      | 1          |
| 1            | 2      | 1          |
|              |        |             |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 2            | 8      | 1          |
| 3            | 8      | 1          |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let count = Count::new(None, AggregateFunction::new_avg());
        for b in res {
            let p = b.columns()[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values();

            count.evaluate(&b, p).unwrap();
        }

        let res = count.finalize();
        println!("{:?}", res);
    }
}
