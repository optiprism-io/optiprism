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

#[derive(Debug)]
struct Group {
    count: i64,
    outer_fn: AggregateFunction,
    first: bool,
    last_partition: i64,
}

impl Group {
    pub fn new(outer_fn: AggregateFunction) -> Self {
        Self {
            count: 0,
            outer_fn,
            first: true,
            last_partition: 0,
        }
    }
}

struct Groups {
    columns: Vec<Column>,
    sort_fields: Vec<SortField>,
    row_converter: RowConverter,
    groups: HashMap<OwnedRow, Group, RandomState>,
}

pub struct Count {
    filter: Option<PhysicalExprRef>,
    outer_fn: AggregateFunction,
    groups: Option<Groups>,
    single_group: Group,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
}

impl Count {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction,
        groups: Option<(Vec<(Column, SortField)>)>,
        partition_col: Column,
    ) -> Result<Self> {
        let groups = if let Some(pairs) = groups {
            Some(Groups {
                columns: pairs.iter().map(|(c, _)| c.clone()).collect(),
                sort_fields: pairs.iter().map(|(_, s)| s.clone()).collect(),
                row_converter: RowConverter::new(pairs.iter().map(|(_, s)| s.clone()).collect())?,
                groups: Default::default(),
            })
        } else {
            None
        };

        Ok(Self {
            filter,
            outer_fn: outer_fn.clone(),
            groups,
            single_group: Group::new(outer_fn),
            partition_col,
            skip: false,
            skip_partition: 0,
        })
    }
}

impl PartitionedAggregateExpr for Count {
    fn group_columns(&self) -> Vec<Column> {
        if let Some(groups) = &self.groups {
            groups.columns.clone()
        } else {
            vec![]
        }
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

        let rows = if let Some(groups) = &mut self.groups {
            let arrs = groups
                .columns
                .iter()
                .map(|e| {
                    e.evaluate(batch)
                        .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
                })
                .collect::<result::Result<Vec<_>, _>>()?;

            Some(groups.row_converter.convert_columns(&arrs)?)
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

            let bucket = if let Some(groups) = &mut self.groups {
                groups
                    .groups
                    .entry(rows.as_ref().unwrap().row(row_id).owned())
                    .or_insert_with(|| {
                        let mut bucket = Group::new(self.outer_fn.clone());
                        bucket
                    })
            } else {
                &mut self.single_group
            };

            if bucket.first {
                bucket.first = false;
                bucket.last_partition = partition;
            }

            if let Some(filter) = &filter {
                if !check_filter(filter, row_id) {
                    continue;
                }
            }

            if bucket.last_partition != partition {
                let v = bucket.count as i128;
                bucket.outer_fn.accumulate(v);
                bucket.last_partition = partition;

                bucket.count = 0;
            }
            bucket.count += 1;
        }

        Ok(())
    }

    fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
        if let Some(groups) = &mut self.groups {
            let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
            let mut res_col_b = Decimal128Builder::with_capacity(groups.groups.len());
            for (row, group) in groups.groups.iter_mut() {
                rows.push(row.row());
                group.outer_fn.accumulate(group.count as i128);
                let res = group.outer_fn.result();
                res_col_b.append_value(res);
            }

            let group_col = groups.row_converter.convert_rows(rows)?;
            let res_col = res_col_b
                .finish()
                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
            let res_col = Arc::new(res_col) as ArrayRef;
            Ok(vec![group_col, vec![res_col]].concat())
        } else {
            let mut res_col_b = Decimal128Builder::with_capacity(1);
            res_col_b.append_value(self.single_group.outer_fn.result());
            let res_col = res_col_b
                .finish()
                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
            let res_col = Arc::new(res_col) as ArrayRef;
            Ok(vec![res_col])
        }
    }

    fn make_new(&self) -> Result<Box<dyn PartitionedAggregateExpr>> {
        let groups = if let Some(groups) = &self.groups {
            Some(Groups {
                columns: groups.columns.clone(),
                sort_fields: groups.sort_fields.clone(),
                row_converter: RowConverter::new(groups.sort_fields.clone())?,
                groups: Default::default(),
            })
        } else {
            None
        };
        let c = Count {
            filter: self.filter.clone(),
            outer_fn: self.outer_fn.clone(),
            groups,
            partition_col: self.partition_col.clone(),
            skip: false,
            skip_partition: 0,
            single_group: Group::new(self.outer_fn.clone()),
        };

        Ok(Box::new(c))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::Column;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::count::Count;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

    #[test]
    fn count_sum_grouped() {
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
        let groups = vec![(
            Column::new_with_schema("device", &schema).unwrap(),
            SortField::new(DataType::Utf8),
        )];
        let hash = HashMap::from([(0, ()), (1, ()), (4, ())]);
        let mut count = Count::try_new(
            None,
            AggregateFunction::new_avg(),
            Some(groups),
            Column::new_with_schema("user_id", &schema).unwrap(),
        )
        .unwrap();
        for b in res {
            count.evaluate(&b, &hash).unwrap();
        }

        let res = count.finalize();
        println!("{:?}", res);
    }

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
            None,
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
