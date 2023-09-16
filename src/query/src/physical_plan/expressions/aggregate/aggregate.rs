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
use arrow::array::Float32Builder;
use arrow::array::Float64Array;
use arrow::array::Float64Builder;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int32Builder;
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
use arrow::util::pretty::print_batches;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::parquet::format::ColumnChunk;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

use crate::error::Result;
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::check_filter;
use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;

#[derive(Debug)]
struct Group<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    agg: AggregateFunction<T>,
}

impl<T> Group<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    pub fn new(agg: AggregateFunction<T>) -> Self {
        Self { agg }
    }
}

#[derive(Debug)]
pub struct Aggregate<T, OT>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
{
    filter: Option<PhysicalExprRef>,
    groups: Option<Groups<Group<OT>>>,
    single_group: Group<OT>,
    predicate: Column,
    partition_col: Column,
    agg: AggregateFunction<OT>,
    t: PhantomData<T>,
}

impl<T, OT> Aggregate<T, OT>
where
    T: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
{
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        groups: Option<(Vec<(PhysicalExprRef, String, SortField)>)>,
        partition_col: Column,
        predicate: Column,
        agg: AggregateFunction<OT>,
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
    ($ty:ident,$array_ty:ident,$acc_ty:ident,$b:ident,$dt:ident) => {
        impl PartitionedAggregateExpr for Aggregate<$ty, $acc_ty> {
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
                let field = Field::new("agg", DataType::$dt, true);
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

                let a = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any();

                print_batches(&[batch.to_owned()]).unwrap();
                println!(
                    "{:?}",
                    predicate = self.predicate.evaluate(batch)?.into_array(batch.num_rows())
                );
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

                    println!("asd {:?}", val.unwrap());
                    bucket.agg.accumulate(val.unwrap() as $acc_ty);
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = $b::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        let res = group.agg.result();
                        res_col_b.append_value(res);
                    }

                    let group_col = groups.row_converter.convert_rows(rows)?;
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![group_col, vec![res_col]].concat())
                } else {
                    let mut res_col_b = $b::with_capacity(1);
                    res_col_b.append_value(self.single_group.agg.result());
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
                let c = Aggregate::<$ty, $acc_ty> {
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

macro_rules! agg_decimal {
    ($ty:ident,$array_ty:ident) => {
        impl PartitionedAggregateExpr for Aggregate<$ty, i128> {
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

                let a = self
                    .predicate
                    .evaluate(batch)?
                    .into_array(batch.num_rows())
                    .as_any();

                print_batches(&[batch.to_owned()]).unwrap();
                println!(
                    "{:?}",
                    predicate = self.predicate.evaluate(batch)?.into_array(batch.num_rows())
                );
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
                    let mut res_col_b = Decimal128Builder::with_capacity(groups.groups.len())
                        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        let res = group.agg.result() * 10i64.pow(DECIMAL_SCALE as u32) as i128;
                        res_col_b.append_value(res);
                    }

                    let group_col = groups.row_converter.convert_rows(rows)?;
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![group_col, vec![res_col]].concat())
                } else {
                    let mut res_col_b = Decimal128Builder::with_capacity(1)
                        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
                    res_col_b.append_value(
                        self.single_group.agg.result() * 10i64.pow(DECIMAL_SCALE as u32) as i128,
                    );
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
                let c = Aggregate::<$ty, i128> {
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

//
agg!(i8, Int8Array, i64, Int64Builder, Int64);
agg!(i16, Int16Array, i64, Int64Builder, Int64);
agg!(i32, Int32Array, i64, Int64Builder, Int64);
agg_decimal!(i64, Int64Array);
agg_decimal!(i128, Decimal128Array);
agg!(u8, UInt8Array, u64, UInt64Builder, UInt64);
agg!(u16, UInt16Array, u64, UInt64Builder, UInt64);
agg!(u32, UInt32Array, u64, UInt64Builder, UInt64);
agg!(u64, UInt64Array, u64, UInt64Builder, UInt64);
agg_decimal!(u128, Decimal128Array);
agg!(f32, Float32Array, f64, Float64Builder, Float64);
agg!(f64, Float64Array, f64, Float64Builder, Float64);
// agg!(Decimal128Array, Decimal128Array, i128);

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use arrow::util::pretty::print_batches;
    use common::DECIMAL_PRECISION;
    use common::DECIMAL_SCALE;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::aggregate2::Aggregate;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;

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
        let mut agg = Aggregate::<i64, i128>::try_new(
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

        let res = agg.finalize().unwrap();

        let schema = Schema::new_with_metadata(
            vec![
                Field::new("f", DataType::Utf8, true),
                agg.fields()[0].clone(),
            ],
            Default::default(),
        );
        println!("{:?}", res);
        let batch = RecordBatch::try_new(Arc::new(schema), res).unwrap();
        print_batches(vec![batch].as_ref()).unwrap();
    }
}
