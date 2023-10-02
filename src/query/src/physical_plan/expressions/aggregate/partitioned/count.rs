use std::collections::HashMap;
use std::result;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Decimal128Builder;
use arrow::array::Float64Builder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::row::Row;
use arrow::row::SortField;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
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
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display
{
    count: i64,
    outer_fn: AggregateFunction<T>,
    first: bool,
    last_partition: i64,
}

impl<T> Group<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display
{
    pub fn new(outer_fn: AggregateFunction<T>) -> Self {
        Self {
            count: 0,
            outer_fn,
            first: true,
            last_partition: 0,
        }
    }
}

#[derive(Debug)]
pub struct PartitionedCount<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display
{
    filter: Option<PhysicalExprRef>,
    outer_fn: AggregateFunction<T>,
    groups: Option<Groups<Group<T>>>,
    single_group: Group<T>,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
    distinct: bool,
}

impl<T> PartitionedCount<T>
where T: Copy + Num + Bounded + NumCast + PartialOrd + Clone + std::fmt::Display
{
    pub fn try_new(
        filter: Option<PhysicalExprRef>,
        outer_fn: AggregateFunction<T>,
        groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
        partition_col: Column,
        distinct: bool,
    ) -> Result<Self> {
        Ok(Self {
            filter,
            outer_fn: outer_fn.make_new(),
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(outer_fn),
            partition_col,
            skip: false,
            skip_partition: 0,
            distinct,
        })
    }
}

macro_rules! count {
    ($acc_ty:ident,$b:ident,$dt:ident) => {
        impl PartitionedAggregateExpr for PartitionedCount<$acc_ty> {
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
                let field = Field::new("partitioned_count", DataType::$dt, true);
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
                                let bucket = Group::new(self.outer_fn.make_new());
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
                        let v = bucket.count;
                        bucket.outer_fn.accumulate(v as $acc_ty);
                        bucket.last_partition = partition;

                        bucket.count = 0;
                    }
                    bucket.count += 1;
                    if self.distinct {
                        self.skip = true;
                        continue;
                    }
                }

                Ok(())
            }

            fn finalize(&mut self) -> Result<Vec<ArrayRef>> {
                if let Some(groups) = &mut self.groups {
                    let mut rows: Vec<Row> = Vec::with_capacity(groups.groups.len());
                    let mut res_col_b = $b::with_capacity(groups.groups.len());
                    for (row, group) in groups.groups.iter_mut() {
                        rows.push(row.row());
                        group.outer_fn.accumulate(group.count as $acc_ty);
                        let res = group.outer_fn.result();

                        res_col_b.append_value(res);
                    }

                    let group_col = groups.row_converter.convert_rows(rows)?;
                    let res_col = res_col_b.finish();
                    let res_col = Arc::new(res_col) as ArrayRef;
                    Ok(vec![group_col, vec![res_col]].concat())
                } else {
                    let mut res_col_b = $b::with_capacity(1);
                    self.single_group
                        .outer_fn
                        .accumulate(self.single_group.count as $acc_ty);
                    let res = self.single_group.outer_fn.result();
                    res_col_b.append_value(res);
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
                let c = PartitionedCount {
                    filter: self.filter.clone(),
                    outer_fn: self.outer_fn.make_new(),
                    groups,
                    partition_col: self.partition_col.clone(),
                    skip: false,
                    skip_partition: 0,
                    single_group: Group::new(self.outer_fn.make_new()),
                    distinct: self.distinct.clone(),
                };

                Ok(Box::new(c))
            }
        }
    };
}

count!(i64, Int64Builder, Int64);
count!(f64, Float64Builder, Float64);

impl PartitionedAggregateExpr for PartitionedCount<i128> {
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
            "partitioned_count",
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
                .map(|e| e.evaluate(batch).map(|v| v.into_array(batch.num_rows())))
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
                    .or_insert_with(|| Group::new(self.outer_fn.make_new()))
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
                let v = bucket.count;
                bucket
                    .outer_fn
                    .accumulate(v as i128 * 10_i128.pow(DECIMAL_SCALE as u32));
                bucket.last_partition = partition;

                bucket.count = 0;
            }
            bucket.count += 1;
            if self.distinct {
                self.skip = true;
                continue;
            }
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
                group
                    .outer_fn
                    .accumulate(group.count as i128 * 10_i128.pow(DECIMAL_SCALE as u32));
                let res = group.outer_fn.result();
                res_col_b.append_value(res);
            }

            let group_col = groups.row_converter.convert_rows(rows)?;
            let res_col = res_col_b.finish();
            let res_col = Arc::new(res_col) as ArrayRef;
            Ok(vec![group_col, vec![res_col]].concat())
        } else {
            let mut res_col_b = Decimal128Builder::with_capacity(1)
                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
            self.single_group
                .outer_fn
                .accumulate(self.single_group.count as i128 * 10_i128.pow(DECIMAL_SCALE as u32));
            let res = self.single_group.outer_fn.result();
            res_col_b.append_value(res);
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
        let c = PartitionedCount {
            filter: self.filter.clone(),
            outer_fn: self.outer_fn.make_new(),
            groups,
            partition_col: self.partition_col.clone(),
            skip: false,
            skip_partition: 0,
            single_group: Group::new(self.outer_fn.make_new()),
            distinct: self.distinct,
        };

        Ok(Box::new(c))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use arrow::row::SortField;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExprRef;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::partitioned::count::PartitionedCount;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
    use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;

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
        let schema = res[0].schema();
        let groups = vec![(
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_owned(),
            SortField::new(DataType::Utf8),
        )];
        let hash = HashMap::from_iter([(0, ()), (1, ()), (4, ())]);
        let mut count = PartitionedCount::<f64>::try_new(
            None,
            AggregateFunction::new_avg(),
            Some(groups),
            Column::new_with_schema("user_id", &schema).unwrap(),
            false,
        )
        .unwrap();
        for b in res {
            count.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = count.finalize();
    }

    #[test]
    fn count_min() {
        let data = r#"
| user_id(i64) | v(i64)
|--------------|-------
| 0            | 1
| 1            | 1
| 1            | 1
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let hash = HashMap::from_iter([(0, ()), (1, ()), (4, ())]);
        let mut agg = PartitionedCount::<f64>::try_new(
            None,
            AggregateFunction::new_avg(),
            None,
            Column::new_with_schema("user_id", &schema).unwrap(),
            false,
        )
        .unwrap();
        for b in res {
            agg.evaluate(&b, Some(&hash)).unwrap();
        }

        let res = agg.finalize().unwrap();
    }
}
