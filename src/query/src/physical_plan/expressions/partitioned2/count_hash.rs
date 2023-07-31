use std::collections::HashMap;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;

use ahash::AHasher;
use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Builder;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use arrow::row::Row;
use arrow::row::RowConverter;
use arrow::row::SortField;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

use crate::error::Result;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::partitioned2::AggregateFunction;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

struct Bucket {
    count: i64,
    outer_fn: AggregateFunction,
    first: bool,
    last_partition: i64,
}

impl Bucket {
    pub fn new(outer_fn: AggregateFunction) -> Self {
        Self {
            count: 0,
            outer_fn,
            first: true,
            last_partition: 0,
        }
    }
}

pub struct Count {
    filter: Option<PhysicalExprRef>,
    outer_fn: AggregateFunction,
    group_cols: Vec<Column>,
    buckets: HashMap<OwnedRow, Bucket, RandomState>,
    row_converter: RowConverter,
}

impl Count {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction,
        groups: Vec<SortField>,
        group_cols: Vec<Column>,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            outer_fn,
            group_cols,
            buckets: HashMap::default(),
            row_converter: RowConverter::new(groups)?,
        })
    }
}

impl PartitionedAggregateExpr for Count {
    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partitions: &ScalarBuffer<i64>,
    ) -> crate::Result<()> {
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

        let arrs = self
            .group_cols
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
            })
            .collect::<result::Result<Vec<_>, _>>()?;

        let rows = self.row_converter.convert_columns(&arrs)?;

        for (row_id, partition) in partitions.into_iter().enumerate() {
            let bucket = self
                .buckets
                .entry(rows.row(row_id).owned())
                .or_insert_with(|| {
                    let mut bucket = Bucket::new(self.outer_fn.clone());
                    bucket
                });

            if bucket.first {
                bucket.first = false;
                bucket.last_partition = *partition;
            }

            if let Some(filter) = &filter {
                if !check_filter(filter, row_id) {
                    continue;
                }
            }

            if bucket.last_partition != *partition {
                let v = bucket.count as i128;
                bucket.outer_fn.accumulate(v);
                bucket.last_partition = *partition;

                bucket.count = 0;
            }
            bucket.count += 1;
        }

        Ok(())
    }

    fn data_types(&self) -> Vec<DataType> {
        vec![DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)]
    }

    fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
        let mut rows: Vec<Row> = Vec::with_capacity(self.buckets.len());
        let mut res_col_b = Decimal128Builder::with_capacity(self.buckets.len());
        for (row, bucket) in self.buckets.iter_mut() {
            rows.push(row.row());
            bucket.outer_fn.accumulate(bucket.count as i128);
            let res = bucket.outer_fn.result();
            res_col_b.append_value(res);
        }

        let group_col = self.row_converter.convert_rows(rows)?;
        let res_col = res_col_b
            .finish()
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
        let res_col = Arc::new(res_col) as ArrayRef;
        Ok(vec![group_col, vec![res_col]].concat())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::row::SortField;
    use datafusion::physical_expr::expressions::Column;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::count_hash::Count;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

    #[test]
    fn count_sum() {
        let data = r#"
| user_id(i64) | device(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|-------|--------|-------------|
| 0            | iphone       | 1     | 1      | e1          |
| 0            | iphone       | 0     | 2      | e2          |
| 0            | iphone       | 0     | 3      | e3          |
| 0            | android      | 1     | 4      | e1          |
| 0            | android      | 1     | 5      | e2          |
| 0            | android      | 0     | 6      | e3          |
| 1            | osx          | 1     | 1      | e1          |
| 1            | osx          | 1     | 2      | e2          |
| 1            | osx          | 0     | 3      | e3          |
| 1            | osx          | 0     | 4      | e1          |
| 1            | osx          | 0     | 5      | e2          |
| 1            | osx          | 0     | 6      | e3          |
| 2            | osx          | 1     | 1      | e1          |
| 2            | osx          | 1     | 2      | e2          |
| 2            | osx          | 0     | 3      | e3          |
| 2            | osx          | 0     | 4      | e1          |
| 2            | osx          | 0     | 5      | e2          |
| 2            | osx          | 0     | 6      | e3          |
| 3            | osx          | 1     | 1      | e1          |
| 3            | osx          | 1     | 2      | e2          |
| 3            | osx          | 0     | 3      | e3          |
| 3            | osx          | 0     | 4      | e1          |
| 3            | osx          | 0     | 5      | e2          |
| 3            | osx          | 0     | 6      | e3          |
| 4            | osx          | 0     | 6      | e3          
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();
        let groups = vec![SortField::new(DataType::Utf8)];
        let group_cols = vec![Column::new_with_schema("device", &schema).unwrap()];
        let mut count =
            Count::try_new(None, AggregateFunction::new_avg(), groups, group_cols).unwrap();
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
