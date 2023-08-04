use std::collections::HashMap;
use std::collections::VecDeque;
use std::result;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use arrow_row::OwnedRow;
use arrow_row::SortField;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::physical_plan::expressions::partitioned2::funnel::evaluate_batch;
use crate::physical_plan::expressions::partitioned2::funnel::Batch;
use crate::physical_plan::expressions::partitioned2::funnel::Count;
use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
use crate::physical_plan::expressions::partitioned2::funnel::Filter;
use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
use crate::physical_plan::expressions::partitioned2::funnel::Touch;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

#[derive(Debug)]
struct Step {
    ts: i64,
    row_id: usize,
    batch_id: usize,
    offset: usize,
}

#[derive(Debug)]
struct Row {
    row_id: usize,
    batch_id: usize,
}

#[derive(Debug)]
pub struct Funnel {
    ts_col: Column,
    window: Duration,
    steps_expr: Vec<PhysicalExprRef>,
    steps_orders: Vec<StepOrder>,
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
    first: bool,
    last_partition: i64,
    partition_start: Row,
    partition_len: i64,
    const_row: Option<Row>,
    cur_step: usize,
    first_step: bool,
    steps: Vec<Step>,
    buf: HashMap<usize, Batch, RandomState>,
    batch_id: usize,
    processed_batches: usize,
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
            steps_orders: opts
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
            first: true,
            last_partition: 0,
            partition_len: 0,
            partition_start: Row {
                row_id: 0,
                batch_id: 0,
            },
            const_row: None,
            cur_step: 0,
            first_step: true,
            steps: opts
                .steps
                .iter()
                .map(|_| Step {
                    ts: 0,
                    row_id: 0,
                    batch_id: 0,
                    offset: 0,
                })
                .collect(),
            buf: Default::default(),
            batch_id: 0,
            processed_batches: 0,
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps_orders.len()
    }
}

impl PartitionedAggregateExpr for Funnel {
    fn group_columns(&self) -> Vec<Column> {
        vec![]
    }

    fn fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new("completed", DataType::Boolean, false),
            Field::new("steps", DataType::Int64, false),
        ];

        let mut steps_ts_fields = self
            .steps_orders
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                Field::new(
                    format!("step{idx}_ts"),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                )
            })
            .collect::<Vec<_>>();

        fields.append(&mut steps_ts_fields);

        fields
    }

    fn evaluate(
        &mut self,
        batch: &RecordBatch,
        partition_exist: &HashMap<i64, ()>,
    ) -> crate::Result<()> {
        let funnel_batch = evaluate_batch(
            batch.to_owned(),
            &self.steps_expr,
            &self.exclude_expr,
            &self.constants,
            &self.ts_col,
        )?;
        self.buf.insert(self.batch_id, funnel_batch);
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
                self.partition_start = Row {
                    row_id,
                    batch_id: self.batch_id,
                };
            }

            if self.last_partition != partition {
                self.cur_step = 0;
                // println!("{} {:?}", self.partition_len, self.partition_start);
                let mut cur_row_id = self.partition_start.row_id;
                let mut cur_batch_id = self.partition_start.batch_id;
                let mut batch = self.buf.get(&cur_batch_id).unwrap();
                let mut offset: usize = 0;
                'main: while offset < self.partition_len as usize {
                    let cur_ts = batch.ts.value(cur_row_id);

                    if self.cur_step > 0 {
                        if let Some(exclude) = &batch.exclude {
                            for excl in exclude.iter() {
                                let mut to_check = false;
                                // check if this exclude is relevant to current step
                                if let Some(steps) = &excl.steps {
                                    for pair in steps {
                                        if pair.from <= self.cur_step && pair.to >= self.cur_step {
                                            to_check = true;
                                            break;
                                        }
                                    }
                                } else {
                                    // check anyway
                                    to_check = true;
                                }

                                if to_check {
                                    println!(
                                        "to check {cur_batch_id}  {cur_row_id} {}",
                                        self.cur_step
                                    );
                                    if excl.exists.value(cur_row_id) {
                                        println!("exclude {cur_batch_id}  {cur_row_id}");
                                        self.steps[0].offset = self.steps[self.cur_step].offset;
                                        self.steps[0].row_id = self.steps[self.cur_step].row_id;
                                        self.steps[0].batch_id = self.steps[self.cur_step].batch_id;
                                        self.cur_step = 0;
                                        offset = self.steps[self.cur_step].offset + 1;
                                        cur_row_id = self.steps[self.cur_step].row_id + 1;
                                        cur_batch_id = self.steps[self.cur_step].batch_id;

                                        continue;
                                    }
                                }
                            }
                        }
                        if cur_ts - self.steps[0].ts > self.window.num_milliseconds() {
                            println!("out of window {cur_batch_id} {cur_row_id}");
                            self.cur_step = 0;
                            offset = self.steps[self.cur_step].offset + 1;
                            cur_row_id = self.steps[self.cur_step].row_id + 1;
                            cur_batch_id = self.steps[self.cur_step].batch_id;
                            continue;
                        }
                    }

                    if batch.steps[self.cur_step].value(cur_row_id) {
                        self.first_step = false;
                        println!(
                            "step {} as row {cur_row_id} batch {cur_batch_id}",
                            self.cur_step
                        );

                        self.steps[self.cur_step] = Step {
                            ts: cur_ts,
                            row_id: cur_row_id,
                            batch_id: cur_batch_id,
                            offset,
                        };

                        if self.cur_step == 0 {
                            if batch.constants.is_some() {
                                self.const_row = Some(Row {
                                    row_id: cur_row_id,
                                    batch_id: cur_batch_id,
                                })
                            }
                        } else {
                            // compare current value with constant
                            // get constant row
                            if batch.constants.is_some() {
                                let const_row = self.const_row.as_ref().unwrap();
                                for (const_idx, first_const) in
                                    batch.constants.as_ref().unwrap().iter().enumerate()
                                {
                                    // compare the const values of current row and first row
                                    let cur_const = &batch.constants.as_ref().unwrap()[const_idx];
                                    if !first_const.eq_values(
                                        const_row.row_id,
                                        cur_const,
                                        cur_row_id,
                                    ) {
                                        println!("constant violation {cur_batch_id} {cur_row_id}");
                                        self.cur_step = 0;
                                        self.first_step = true;
                                        offset = self.steps[self.cur_step].offset + 1;
                                        cur_row_id = self.steps[self.cur_step].row_id + 1;
                                        cur_batch_id = self.steps[self.cur_step].batch_id;
                                        continue 'main;
                                    }
                                }
                            }
                        }

                        if self.cur_step >= self.steps.len() - 1 {
                            break;
                        }
                        self.cur_step += 1;
                    }
                    cur_row_id += 1;
                    if cur_row_id >= batch.len() {
                        cur_batch_id += 1;
                        cur_row_id = 0;
                        batch = self.buf.get(&cur_batch_id).unwrap();
                    }
                    offset += 1;
                }
                if self.cur_step == self.steps_count() - 1 {
                    println!("complete!");
                }

                self.partition_len = 0;

                self.partition_start = Row {
                    row_id,
                    batch_id: self.batch_id,
                };
                self.last_partition = partition;
            }

            // println!("{row_id}");
            self.partition_len += 1;
        }

        self.batch_id += 1;

        Ok(())
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        todo!()
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::row::SortField;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::partitioned2::count::Count;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::Options;
    use crate::physical_plan::expressions::partitioned2::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned2::funnel::Touch;
    use crate::physical_plan::expressions::partitioned2::AggregateFunction;
    use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;

    #[test]
    fn test() {
        let data = r#"
| u(i64) | ts(ts) | v(i64) | c(i64) |
|--------|--------|--------|--------|
| 0      | 1      | 1      | 1      |
| 1      | 1      | 2      | 1      |
| 1      | 2      | 1      | 1      |
| 1      | 2      | 1      | 1      |
| 1      | 3      | 2      | 1      |
|        |        |        |        |
| 1      | 4      | 1      | 1      |
| 1      | 5      | 2      | 1      |
| 1      | 5      | 4      | 1      |
| 1      | 6      | 3      | 2      |
| 1      | 4      | 1      | 1      |
| 1      | 5      | 2      | 1      |
| 1      | 6      | 3      | 1      |
| 2      | 1      | 1      | 1      |
| 2      | 2      | 2      | 1      |
| 2      | 3      | 3      | 1      |
|        |        |        |        |
| 3      | 1      | 1      | 1      |
|        |        |        |        |
| 3      | 2      | 2      | 1      |
|        |        |        |        |
| 3      | 3      | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema().clone();
        let hash = HashMap::from([(0, ()), (1, ()), (2, ()), (3, ())]);

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let opts = Options {
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            window: Duration::milliseconds(200),
            steps: vec![e1, e2, e3],
            exclude: None,
            // Some(vec![ExcludeExpr {
            // expr: ex,
            // steps: None,
            // }]),
            constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
        };
        let mut f = Funnel::new(opts);
        for b in res {
            f.evaluate(&b, &hash).unwrap();
        }

        // f.finalize()?;
    }
}
