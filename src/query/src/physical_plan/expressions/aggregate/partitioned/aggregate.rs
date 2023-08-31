use std::collections::HashMap;
use std::marker::PhantomData;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;

use ahash::AHasher;
use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::Int8Array;
use arrow::array::PrimitiveArray;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
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
use crate::physical_plan::expressions::aggregate::AggregateFunction;
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::check_filter;

#[derive(Debug)]
struct Group {
    inner_fn: AggregateFunction,
    outer_fn: AggregateFunction,
    first: bool,
    last_partition: i64,
}

impl Group {
    pub fn new(inner_fn: AggregateFunction, outer_fn: AggregateFunction) -> Self {
        Self {
            inner_fn,
            outer_fn,
            first: true,
            last_partition: 0,
        }
    }
}
#[derive(Debug)]
pub struct Aggregate<T> {
    filter: Option<PhysicalExprRef>,
    predicate: Column,
    typ: PhantomData<T>,
    inner_fn: AggregateFunction,
    outer_fn: AggregateFunction,
    groups: Option<Groups<Group>>,
    single_group: Group,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
}

impl<T> Aggregate<T> {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        inner_fn: AggregateFunction,
        outer_fn: AggregateFunction,
        predicate: Column,
        groups: Option<(Vec<(PhysicalExprRef, String, SortField)>)>,
        partition_col: Column,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            predicate,
            typ: Default::default(),
            inner_fn: inner_fn.make_new(),
            outer_fn: outer_fn.make_new(),
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(inner_fn, outer_fn),
            partition_col,
            skip: false,
            skip_partition: 0,
        })
    }
}

macro_rules! agg {
    ($ty:ident,$array_ty:ident) => {
        impl PartitionedAggregateExpr for Aggregate<$ty> {
            fn group_columns(&self) -> Vec<(PhysicalExprRef, String)> {
                if let Some(groups) = &self.groups {
                    groups
                        .exprs
                        .iter()
                        .zip(groups.names.iter())
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect()
                } else {
                    vec![]
                }
            }

            fn fields(&self) -> Vec<Field> {
                let field = Field::new(
                    "agg",
                    DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                    true,
                );
                vec![field]
            }

            fn evaluate(
                &mut self,
                batch: &RecordBatch,
                partition_exist: Option<&HashMap<i64, (), RandomState>>,
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
                        .exprs
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

                let predicate = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();

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
                    if let Some(exists) = partition_exist {
                        if !exists.contains_key(&partition) {
                            self.skip = true;
                            continue;
                        }
                    }

                    let bucket = if let Some(groups) = &mut self.groups {
                        groups
                            .groups
                            .entry(rows.as_ref().unwrap().row(row_id).owned())
                            .or_insert_with(|| {
                                let mut bucket =
                                    Group::new(self.inner_fn.make_new(), self.outer_fn.make_new());
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
                        let v = bucket.inner_fn.result();
                        bucket.outer_fn.accumulate(v);
                        bucket.last_partition = partition;

                        bucket.inner_fn.reset();
                    }
                    bucket.inner_fn.accumulate(predicate.value(row_id) as i128);
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = Decimal128Builder::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        group.outer_fn.accumulate(group.inner_fn.result() as i128);
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
                    self.single_group
                        .outer_fn
                        .accumulate(self.single_group.inner_fn.result() as i128);
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
                    Some(groups.try_make_new()?)
                } else {
                    None
                };
                let c: Aggregate<$ty> = Aggregate {
                    filter: self.filter.clone(),
                    typ: Default::default(),
                    predicate: self.predicate.clone(),
                    inner_fn: self.inner_fn.make_new(),
                    outer_fn: self.outer_fn.make_new(),
                    groups,
                    partition_col: self.partition_col.clone(),
                    skip: false,
                    skip_partition: 0,
                    single_group: Group::new(self.inner_fn.make_new(), self.outer_fn.make_new()),
                };

                Ok(Box::new(c))
            }
        }
    };
}
agg!(i8, Int8Array);
agg!(i16, Int16Array);
agg!(i32, Int32Array);
agg!(i64, Int64Array);
agg!(i128, Decimal128Array);
agg!(u8, UInt8Array);
agg!(u16, UInt16Array);
agg!(u32, UInt32Array);
agg!(u64, UInt64Array);
agg!(u128, Decimal128Array);
agg!(f32, Float32Array);
agg!(f64, Float64Array);
agg!(Decimal128Array, Decimal128Array);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use ahash::AHasher;
    use ahash::RandomState;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::partitioned::aggregate::Aggregate;
    use crate::physical_plan::expressions::aggregate::AggregateFunction;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;

    #[test]
    fn sum_avg_grouped() {
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
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_owned(),
            SortField::new(DataType::Utf8),
        )];

        let hash = ahash::HashMap::from_iter(vec![(0, ()), (1, ()), (4, ())]);
        let mut agg = Aggregate::<i64>::try_new(
            None,
            AggregateFunction::new_sum(),
            AggregateFunction::new_avg(),
            Column::new_with_schema("v", &schema).unwrap(),
            Some(groups),
            Column::new_with_schema("user_id", &schema).unwrap(),
        )
        .unwrap();
        for b in res {
            agg.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = agg.finalize();
        println!("{:?}", res);
    }

    #[test]
    fn sum_avg() {
        let data = r#"
| user_id(i64) | v(i64) | ts(ts) | event(utf8) |
|--------------|-------|--------|-------------|
| 1            | 1     | 1      | e1          |
| 1            | 1     | 1      | e1          |
| 2            | 0     | 6      | e3
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();

        let hash = ahash::HashMap::from_iter([(1, ()), (2, ())]);
        let mut count = Aggregate::<i64>::try_new(
            None,
            AggregateFunction::new_sum(),
            AggregateFunction::new_avg(),
            Column::new_with_schema("v", &schema).unwrap(),
            None,
            Column::new_with_schema("user_id", &schema).unwrap(),
        )
        .unwrap();
        for b in res {
            count.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = count.finalize();
        println!("{:?}", res);
    }
}
