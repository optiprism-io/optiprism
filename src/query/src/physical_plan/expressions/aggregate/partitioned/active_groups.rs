use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow_row::SortField;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use hyperloglog::HyperLogLog;
use crate::error::Result;
use crate::physical_plan::expressions::aggregate::Groups;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;

#[derive(Debug)]
struct Group {
    first: bool,
    last_partition: i64,
    hll: HyperLogLog,
}

impl Group {
    pub fn new() -> Self {
        Self {
            first: true,
            last_partition: 0,
            hll: HyperLogLog::new(0.05),
        }
    }
}

#[derive(Debug)]
pub struct ActiveGroups {
    groups: Option<Groups<Group>>,
    single_group: Group,
    partition_col: Column,
    ts_col: Column,
    from_ts: i64,
    window: Duration,
    skip: bool,
    skip_partition: i64,
}

impl ActiveGroups {
    pub fn try_new(
        partition_col: Column,
        ts_col: Column,
        from_ts: i64,
        groups: Option<Vec<(PhysicalExprRef, String, SortField)>>,
        window: Duration,
    ) -> Result<Self> {
        Ok(Self {
            groups: Groups::maybe_from(groups)?,
            single_group: Group::new(),
            partition_col,
            ts_col,
            from_ts,
            window,
            skip: false,
            skip_partition: 0,
        })
    }
}

impl PartitionedAggregateExpr for ActiveGroups {
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
        let field = Field::new("active_groups", DataType::Int64, true);
        vec![field]
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: Option<&HashMap<i64, (), RandomState>>,
    ) -> crate::Result<()> {
        let rows = if let Some(groups) = &mut self.groups {
            let arrs = groups
                .exprs
                .iter()
                .map(|e| {
                    e.evaluate(batch)
                        .and_then(|v| Ok(v.into_array(batch.num_rows()).unwrap().clone()))
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Some(groups.row_converter.convert_columns(&arrs)?)
        } else {
            None
        };

        let partitions = self
            .partition_col
            .evaluate(batch)?
            .into_array(batch.num_rows())?
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();

        let mut skip_partition = 0;
        let mut skip = false;
        for (row_id, partition) in partitions.into_iter().enumerate() {
            let partition = partition.unwrap();
            if skip {
                if partition == skip_partition {
                    continue;
                } else {
                    skip = false;
                }
            }
            if let Some(exists) = partition_exist {
                let pid = partition;
                if !exists.contains_key(&pid) {
                    skip = true;
                    skip_partition = pid;
                    continue;
                }
            }

            if let Some(groups) = &mut self.groups {
                groups
                    .groups
                    .entry(rows.as_ref().unwrap().row(row_id).owned())
                    .or_insert_with(|| {
                        let bucket = Group::new();
                        bucket
                    })
            } else {
                &mut self.single_group
            };
        }

        Ok(())
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        todo!()
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        todo!()
    }

    fn merge(&mut self, _other: &dyn PartitionedAggregateExpr) -> crate::Result<()> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn op(&self) -> &str {
        todo!()
    }
}
