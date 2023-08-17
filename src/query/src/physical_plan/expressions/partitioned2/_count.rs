use std::collections::HashMap;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;

use ahash::AHasher;
use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Builder;
use arrow::array::Int64Array;
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::row::OwnedRow;
use arrow::row::Row;
use arrow::row::RowConverter;
use arrow::row::SortField;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::parquet::format::ColumnChunk;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

use crate::error::Result;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::partitioned2::AggregateFunction;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

pub struct Count {
    filter: Option<PhysicalExprRef>,
    outer_fn: AggregateFunction,
    partition_col: Column,
    count: i64,
    first: bool,
    last_partition: i64,
    skip: bool,
    skip_partition: i64,
}

impl Count {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction,
        partition_col: Column,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            outer_fn,
            partition_col,
            count: 0,
            first: false,
            last_partition: 0,
            skip: false,
            skip_partition: 0,
        })
    }
}

impl PartitionedAggregateExpr for Count {
    fn group_columns(&self) -> Vec<Column> {
        vec![]
    }

    fn fields(&self) -> Vec<Field> {
        let field = Field::new(
            "count",
            DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            true,
        );
        vec![field]
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: &HashMap<i64, ()>,
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

        let partitions = self
            .partition_col
            .evaluate(batch)?
            .into_array(batch.num_rows())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();

        for (row_id, partition) in partitions.into_iter().enumerate() {
            let partition = partition.unwrap();
            if self.skip {
                if self.skip_partition != partition {
                    self.skip = false;
                } else {
                    continue;
                }
            }
            if !partition_exist.contains_key(&partition) {
                self.skip = true;
                continue;
            }

            if self.first {
                self.first = false;
                self.last_partition = partition;
            }

            if let Some(filter) = &filter {
                if !check_filter(filter, row_id) {
                    continue;
                }
            }

            if self.last_partition != partition {
                let v = self.count as i128;
                self.outer_fn.accumulate(v);
                self.last_partition = partition;

                self.count = 0;
            }
            self.count += 1;
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
        let mut res_col_b = Decimal128Builder::with_capacity(1);
        res_col_b.append_value(self.outer_fn.result());
        let res_col = res_col_b
            .finish()
            .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
        let res_col = Arc::new(res_col) as ArrayRef;
        Ok(vec![res_col])
    }

    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>> {
        let a = Count {
            filter: self.filter.clone(),
            outer_fn: self.outer_fn.clone(),
            partition_col: self.partition_col.clone(),
            count: 0,
            first: false,
            last_partition: 0,
            skip: false,
            skip_partition: 0,
        };

        Ok(Box::new(a))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::row::SortField;
    use datafusion::physical_expr::expressions::Column;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::partitioned2::_count::Count;

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
| 4            | windows      | 0     | 6      | e3          
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();
        let hash = HashMap::from([(0, ()), (1, ()), (4, ())]);
        let mut count = Count::try_new(
            None,
            AggregateFunction::new_avg(),
            Column::new_with_schema("user_id", &schema).unwrap(),
        )
        .unwrap();
        for b in res {
            count.evaluate(&b, &hash).unwrap();
        }

        let res = count.finalize();
        println!("{:?}", res);
    }
}
