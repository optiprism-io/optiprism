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
    agg: AggregateFunction,
}

impl Group {
    pub fn new(agg: AggregateFunction) -> Self {
        Self { agg }
    }
}
#[derive(Debug)]
pub struct Aggregate<T> {
    filter: Option<PhysicalExprRef>,
    groups: Option<Groups<Group>>,
    single_group: Group,
    predicate: Column,
    partition_col: Column,
    agg: AggregateFunction,
    t: PhantomData<T>,
}

impl<T> Aggregate<T> {
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        groups: Option<(Vec<(PhysicalExprRef, String, SortField)>)>,
        partition_col: Column,
        predicate: Column,
        agg: AggregateFunction,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(agg.make_new()),
            predicate,
            partition_col,
            agg,
            t: Default::default(),
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
                                let mut bucket = Group::new(self.agg.make_new());
                                bucket
                            })
                    } else {
                        &mut self.single_group
                    };

                    bucket.agg.accumulate(val.unwrap() as i128);
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = Decimal128Builder::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        let res = group.agg.result() as i128;
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
                    res_col_b.append_value(self.single_group.agg.result() as i128);
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
                let c = Aggregate::<$ty> {
                    filter: self.filter.clone(),
                    groups,
                    single_group: Group::new(self.agg.make_new()),
                    predicate: self.predicate.clone(),
                    partition_col: self.partition_col.clone(),
                    agg: self.agg.make_new(),
                    t: Default::default(),
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

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::print_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::aggregate::Aggregate;
    use crate::physical_plan::expressions::aggregate::AggregateFunction;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;

    #[test]
    fn sum_grouped() {
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
        let mut agg = Aggregate::<i64>::try_new(
            None,
            Some(groups),
            Column::new_with_schema("user_id", &schema).unwrap(),
            Column::new_with_schema("v", &schema).unwrap(),
            AggregateFunction::new_sum(),
        )
        .unwrap();
        for b in res {
            agg.evaluate(&b, None).unwrap();
        }

        let res = agg.finalize();
        println!("{:?}", res);
    }
}
