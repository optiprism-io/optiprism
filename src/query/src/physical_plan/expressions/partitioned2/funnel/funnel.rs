use std::collections::HashMap;
use std::result;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow_row::OwnedRow;
use arrow_row::SortField;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::physical_plan::expressions::partitioned2::funnel::Count;
use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
use crate::physical_plan::expressions::partitioned2::funnel::Filter;
use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
use crate::physical_plan::expressions::partitioned2::funnel::Touch;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

#[derive(Debug)]
pub struct Funnel {
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps: Vec<StepOrder>,
    exclude_expr: Option<Vec<ExcludeExpr>>,
    // expr and vec of step ids
    constants: Option<Vec<Column>>,
    count: Count,
    // vec of col ids
    filter: Option<Filter>,
    touch: Touch,
    partition_col: Column,
    skip: bool,
    skip_partition: i64,
}

#[derive(Debug, Clone)]
pub struct Options {
    pub ts_col: Column,
    pub window: Duration,
    pub steps: Vec<(PhysicalExprRef, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Column>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub partition_col: Column,
}

impl Funnel {
    pub fn new(opts: Options) -> Self {
        Self {
            ts_col: opts.ts_col,
            window: opts.window,
            steps_expr: opts
                .steps
                .iter()
                .map(|(expr, _)| expr.clone())
                .collect::<Vec<_>>(),
            steps: opts
                .steps
                .iter()
                .map(|(_, order)| order.clone())
                .collect::<Vec<_>>(),
            exclude_expr: opts.exclude,
            constants: opts.constants,
            count: opts.count,
            filter: opts.filter,
            touch: opts.touch,
            partition_col: opts.partition_col,
            skip: false,
            skip_partition: 0,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps.len()
    }
}

impl PartitionedAggregateExpr for Funnel {
    fn group_columns(&self) -> Vec<Column> {
        vec![]
    }

    fn fields(&self) -> Vec<Field> {
        todo!()
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: &HashMap<i64, ()>,
    ) -> crate::Result<()> {
        let arrs = self
            .group_cols
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| Ok(v.into_array(batch.num_rows()).clone()))
            })
            .collect::<result::Result<Vec<_>, _>>()?;

        let rows = self.row_converter.convert_columns(&arrs)?;

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

            let buckets = self
                .buckets
                .entry(rows.row(row_id).owned())
                .or_insert_with(|| {
                    let mut buckets = self.buckets.make_new();

                    buckets
                });

            if buckets.first {
                buckets.first = false;
                buckets.last_partition = partition;
            }
        }

        sdf
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        todo!()
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        todo!()
    }
}
