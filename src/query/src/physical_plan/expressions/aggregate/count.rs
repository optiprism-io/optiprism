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
use arrow::array::UInt64Builder;
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
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::check_filter;

#[derive(Debug)]
struct Group {
    count: i64,
}

impl Group {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}
#[derive(Debug)]
pub struct Count<T> {
    filter: Option<PhysicalExprRef>,
    groups: Option<Groups<Group>>,
    single_group: Group,
    distinct: bool,
    predicate: Column,
    partition_col: Column,
    t: PhantomData<T>,
}

impl<T> Count<T> {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        groups: Option<(Vec<(PhysicalExprRef, String, SortField)>)>,
        predicate: Column,
        partition_col: Column,
        distinct: bool,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(),
            distinct,
            predicate,
            partition_col,
            t: Default::default(),
        })
    }
}
macro_rules! count {
    ($ty:ident,$array_ty:ident) => {
        impl PartitionedAggregateExpr for Count<$ty> {
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
                let field = Field::new("count", DataType::UInt64, true);
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

                let predicate = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();

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

                let partitions = self
                    .partition_col
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();

                let mut skip_partition = 0;
                let mut skip = false;
                for (row_id, val) in predicate.into_iter().enumerate() {
                    if skip {
                        if partitions.value(row_id) == skip_partition {
                            continue;
                        } else {
                            skip = false;
                        }
                    }
                    if let Some(exists) = partition_exist {
                        let pid = partitions.value(row_id);
                        if !exists.contains_key(&pid) {
                            skip = true;
                            skip_partition = pid;
                            continue;
                        }
                    }

                    if let Some(filter) = &filter {
                        if !check_filter(filter, row_id) {
                            continue;
                        }
                    }

                    if val.is_none() {
                        continue;
                    }

                    let bucket = if let Some(groups) = &mut self.groups {
                        groups
                            .groups
                            .entry(rows.as_ref().unwrap().row(row_id).owned())
                            .or_insert_with(|| {
                                let mut bucket = Group::new();
                                bucket
                            })
                    } else {
                        &mut self.single_group
                    };

                    bucket.count += 1;
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = UInt64Builder::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        let res = group.count as u64;
                        res_col_b.append_value(res);
                    }

                    let group_col = groups.row_converter.convert_rows(rows)?;
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![group_col, vec![res_col]].concat())
                } else {
                    let mut res_col_b = UInt64Builder::with_capacity(1);
                    res_col_b.append_value(self.single_group.count as u64);
                    let res_col = res_col_b.finish();
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
                let c = Count::<$ty> {
                    filter: self.filter.clone(),
                    groups,
                    single_group: Group::new(),
                    distinct: self.distinct.clone(),
                    predicate: self.predicate.clone(),
                    partition_col: self.partition_col.clone(),
                    t: Default::default(),
                };

                Ok(Box::new(c))
            }
        }
    };
}

count!(i8, Int8Array);
count!(i16, Int16Array);
count!(i32, Int32Array);
count!(i64, Int64Array);
count!(i128, Decimal128Array);
count!(u8, UInt8Array);
count!(u16, UInt16Array);
count!(u32, UInt32Array);
count!(u64, UInt64Array);
count!(u128, Decimal128Array);
count!(f32, Float32Array);
count!(f64, Float64Array);
count!(Decimal128Array, Decimal128Array);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::count::Count;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;

    #[test]
    fn count_grouped() {
        let data = r#"
| user_id(i64)| device(utf8) | v(i64)| event(utf8) |
|-------------|--------------|-------|-------------|
| 0           | iphone       | 1     | e1          |
| 0           | android      | 1     | e1          |
| 0           | android      | 1     | e1          |
| 0           | osx          | 1     | e1          |
| 0           | osx          | 1     | e3          |
| 0           | osx          | 1     | e3          |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();
        let groups = vec![(
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_string(),
            SortField::new(DataType::Utf8),
        )];
        let mut count = Count::<i64>::try_new(
            None,
            Some(groups),
            Column::new_with_schema("v", &schema).unwrap(),
            Column::new_with_schema("user_id", &schema).unwrap(),
            false,
        )
        .unwrap();
        for b in res {
            count.evaluate(&b, None).unwrap();
        }

        let res = count.finalize();
        println!("{:?}", res);
    }
}
