use std::collections::HashMap;
use std::collections::VecDeque;
use std::result;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
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

use crate::error::QueryError;
use crate::physical_plan::expressions::partitioned2::funnel::evaluate_batch;
use crate::physical_plan::expressions::partitioned2::funnel::Batch;
use crate::physical_plan::expressions::partitioned2::funnel::Count;
use crate::physical_plan::expressions::partitioned2::funnel::Exclude;
use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
use crate::physical_plan::expressions::partitioned2::funnel::Filter;
use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
use crate::physical_plan::expressions::partitioned2::funnel::Touch;
use crate::physical_plan::expressions::partitioned2::PartitionedAggregateExpr;
use crate::StaticArray;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DebugStep {
    Step,
    NextRow,
    ExcludeViolation,
    OutOfWindow,
    ConstantViolation,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Eq, PartialEq)]
struct StepResult {
    count: usize,
    total_time: i64,
    total_time_from_start: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FunnelResult {
    total_funnels: usize,
    completed_funnels: usize,
    steps: Vec<StepResult>,
}

impl FunnelResult {
    // add result to self
    pub fn add(&self, other: FunnelResult) -> FunnelResult {
        let mut res = FunnelResult {
            total_funnels: self.total_funnels + other.total_funnels,
            completed_funnels: self.completed_funnels + other.completed_funnels,
            steps: vec![],
        };

        for (idx, step) in self.steps.iter().enumerate() {
            res.steps.push(StepResult {
                count: step.count + other.steps[idx].count,
                total_time: step.total_time + other.steps[idx].total_time,
                total_time_from_start: step.total_time_from_start
                    + other.steps[idx].total_time_from_start,
            })
        }

        res
    }
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
    cur_partition: i64,
    cur_row_id: i64,
    partition_start: Row,
    partition_len: i64,
    const_row: Option<Row>,
    cur_step: usize,
    first_step: bool,
    steps: Vec<Step>,
    buf: HashMap<usize, Batch, RandomState>,
    batch_id: usize,
    processed_batches: usize,
    debug: Vec<DebugStep>,
    result: FunnelResult,
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

struct PartitionRow<'a> {
    row_id: usize,
    batch_id: usize,
    batch: &'a Batch,
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
            cur_partition: 0,
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
            cur_row_id: 0,
            debug: Vec::with_capacity(100),
            result: FunnelResult {
                total_funnels: 0,
                completed_funnels: 0,
                steps: (0..opts.steps.len())
                    .into_iter()
                    .map(|_| StepResult {
                        count: 0,
                        total_time: 0,
                        total_time_from_start: 0,
                    })
                    .collect::<Vec<_>>(),
            },
        }
    }

    pub fn steps_count(&self) -> usize {
        self.steps_orders.len()
    }

    fn check_partition_bounds(
        &mut self,
        row_id: usize,
        partition: i64,
        partition_exist: &HashMap<i64, ()>,
    ) -> bool {
        if self.skip {
            if self.skip_partition != partition {
                self.skip = false;
            } else {
                return false;
            }
        }

        if !partition_exist.contains_key(&partition) {
            self.skip = true;
            return false;
        }

        if self.first {
            self.first = false;
            self.cur_partition = partition;
            self.partition_start = Row {
                row_id,
                batch_id: self.batch_id,
            };
        }

        true
    }

    fn check_exclude(&self, exclude: &Vec<Exclude>, cur_row_id: usize) -> bool {
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
                if excl.exists.value(cur_row_id) {
                    return false;
                }
            }
        }

        true
    }

    fn check_constants(&self, constants: &Vec<StaticArray>, cur_row_id: usize) -> bool {
        let const_row = self.const_row.as_ref().unwrap();
        for (const_idx, first_const) in constants.iter().enumerate() {
            // compare the const values of current row and first row
            let cur_const = &constants[const_idx];
            if !first_const.eq_values(const_row.row_id, cur_const, cur_row_id) {
                return false;
            }
        }

        true
    }

    pub fn process_partition(&mut self, row_id: usize, partition: i64) {
        self.cur_step = 0;
        let mut cur_row_id = self.partition_start.row_id;
        let mut cur_batch_id = self.partition_start.batch_id;
        let mut batch = self.buf.get(&cur_batch_id).unwrap();
        let mut offset: usize = 0;

        while offset < self.partition_len as usize {
            let cur_ts = batch.ts.value(cur_row_id);
            println!(
                "pk {} ts {cur_ts} row {cur_row_id} batch {cur_batch_id} offset {offset}",
                self.cur_partition
            );

            if self.cur_step > 0 {
                if let Some(exclude) = &batch.exclude {
                    if !self.check_exclude(exclude, cur_row_id) {
                        self.debug.push(DebugStep::ExcludeViolation);
                        println!("exclude {cur_batch_id}  {cur_row_id}");
                        self.steps[0] = self.steps[self.cur_step].clone();
                        self.cur_step = 0;

                        // continue, so this row will be processed twice, as possible first step as well
                        continue;
                    }
                }

                if cur_ts - self.steps[0].ts > self.window.num_milliseconds() {
                    println!("out of window {cur_batch_id} {cur_row_id}");
                    self.debug.push(DebugStep::OutOfWindow);
                    self.cur_step = 0;
                    offset = self.steps[self.cur_step].offset + 1;
                    cur_row_id = self.steps[self.cur_step].row_id + 1;
                    cur_batch_id = self.steps[self.cur_step].batch_id;

                    continue;
                }
            }

            if batch.steps[self.cur_step].value(cur_row_id) {
                self.debug.push(DebugStep::Step);

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
                    if let Some(constants) = &batch.constants {
                        if !self.check_constants(&constants, cur_row_id) {
                            self.debug.push(DebugStep::ConstantViolation);
                            println!("constant violation {cur_batch_id} {cur_row_id}");
                            self.steps[0] = self.steps[self.cur_step].clone();
                            self.cur_step = 0;

                            continue;
                        }
                    }
                }

                if self.cur_step >= self.steps.len() - 1 {
                    break;
                }
                self.cur_step += 1;
            }
            cur_row_id += 1;
            self.debug.push(DebugStep::NextRow);
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
        self.cur_partition = partition;
    }

    fn result(&mut self) -> FunnelResult {
        let is_completed = match &self.filter {
            // if no filter, then funnel is completed id all steps are completed
            None => self.cur_step == self.steps_count() - 1,
            Some(filter) => match filter {
                Filter::DropOffOnAnyStep => self.cur_step != self.steps_count() - 1,
                // drop off on defined step
                Filter::DropOffOnStep(drop_off_step_id) => self.cur_step == *drop_off_step_id,
                // drop off if time to convert is out of range
                Filter::TimeToConvert(from, to) => {
                    if self.cur_step != self.steps_count() - 1 {
                        false
                    } else {
                        let diff = self.steps[self.cur_step].ts - self.steps[0].ts;
                        from.num_milliseconds() <= diff && diff <= to.num_milliseconds()
                    }
                }
            },
        };

        self.result.total_funnels += 1;
        if is_completed {
            self.result.completed_funnels += 1;
        }

        let mut result = FunnelResult {
            total_funnels: 0,
            completed_funnels: 0,
            steps: (0..self.steps.len())
                .into_iter()
                .map(|_| StepResult {
                    count: 0,
                    total_time: 0,
                    total_time_from_start: 0,
                })
                .collect::<Vec<_>>(),
        };

        for step_id in 0..=self.cur_step {
            result.steps[step_id].count += 1;
            if step_id > 0 {
                result.steps[step_id].total_time +=
                    self.steps[step_id].ts - self.steps[step_id - 1].ts;
                result.steps[step_id].total_time_from_start +=
                    self.steps[step_id].ts - self.steps[0].ts;
            }
        }

        result
    }
}

impl PartitionedAggregateExpr for Funnel {
    fn group_columns(&self) -> Vec<Column> {
        vec![]
    }

    fn fields(&self) -> Vec<Field> {
        let mut fields = vec![
            Field::new("total", DataType::Int64, false),
            Field::new("completed", DataType::Int64, false),
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
        self.debug.clear();
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
            if !self.check_partition_bounds(row_id, partition, partition_exist) {
                continue;
            }

            if !partition_exist.contains_key(&partition) {
                self.skip = true;
                continue;
            }

            if self.cur_partition != partition {
                self.process_partition(row_id, partition);
                let result = self.result();
                self.result = self.result.add(result);
            }

            // println!("{row_id}");
            self.partition_len += 1;
        }

        self.batch_id += 1;

        Ok(())
    }

    fn finalize(&mut self) -> crate::Result<Vec<ArrayRef>> {
        self.process_partition(0, 0);
        let result = self.result();
        // make it stateful so we can test it
        self.result = self.result.add(result);
        let result = self.result.clone();

        let mut total_funnels = Int64Builder::new();
        let mut completed_funnels = Int64Builder::new();
        let mut steps = self
            .steps
            .iter()
            .map(|_| Int64Builder::new())
            .collect::<Vec<_>>();

        total_funnels.append_value(result.total_funnels as i64);
        completed_funnels.append_value(result.completed_funnels as i64);
        for (step_id, step) in result.steps.iter().enumerate() {
            steps[step_id].append_value(step.count as i64);
        }

        let arr = vec![
            vec![Arc::new(total_funnels.finish()) as ArrayRef],
            vec![Arc::new(completed_funnels.finish()) as ArrayRef],
            steps
                .iter_mut()
                .map(|b| Arc::new(b.finish()) as ArrayRef)
                .collect::<Vec<_>>(),
        ]
        .concat();
        Ok(arr)
    }

    fn make_new(&self) -> crate::Result<Box<dyn PartitionedAggregateExpr>> {
        let res = Self {
            ts_col: self.ts_col.clone(),
            window: self.window.clone(),
            steps_expr: self.steps_expr.clone(),
            steps_orders: self.steps_orders.clone(),
            exclude_expr: self.exclude_expr.clone(),
            constants: self.constants.clone(),
            count: self.count.clone(),
            filter: self.filter.clone(),
            touch: self.touch.clone(),
            partition_col: self.partition_col.clone(),
            skip: false,
            skip_partition: 0,
            first: true,
            cur_partition: 0,
            cur_row_id: 0,
            partition_start: Row {
                row_id: 0,
                batch_id: 0,
            },
            partition_len: 0,
            const_row: None,
            cur_step: 0,
            first_step: true,
            steps: self
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
            debug: vec![],
            result: FunnelResult {
                total_funnels: 0,
                completed_funnels: 0,
                steps: (0..self.steps.len())
                    .into_iter()
                    .map(|_| StepResult {
                        count: 0,
                        total_time: 0,
                        total_time_from_start: 0,
                    })
                    .collect::<Vec<_>>(),
            },
        };

        Ok(Box::new(res))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use arrow::row::SortField;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_table_v1;
    use store::test_util::parse_markdown_tables;
    use tracing_test::traced_test;

    use crate::event_eq;
    use crate::expected_debug;
    use crate::physical_plan::expressions::partitioned2::funnel::event_eq_;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::DebugStep;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::FunnelResult;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::Options;
    use crate::physical_plan::expressions::partitioned2::funnel::funnel::StepResult;
    use crate::physical_plan::expressions::partitioned2::funnel::Count;
    use crate::physical_plan::expressions::partitioned2::funnel::Count::Unique;
    use crate::physical_plan::expressions::partitioned2::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder;
    use crate::physical_plan::expressions::partitioned2::funnel::StepOrder::Sequential;
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
| 1      | 3      | 1      | 1      |
| 1      | 4      | 2      | 1      |
|        |        |        |        |
| 1      | 5      | 1      | 1      |
| 1      | 6      | 2      | 1      |
| 1      | 7      | 4      | 1      |
| 1      | 8      | 3      | 2      |
| 1      | 9      | 1      | 1      |
| 1      | 10     | 2      | 1      |
| 1      | 11     | 3      | 1      |
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
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
        };
        let mut f = Funnel::new(opts);
        for b in res {
            f.evaluate(&b, &hash).unwrap();
        }

        let res = f.finalize().unwrap();
        println!("{:?}", res);
        // f.finalize()?;
    }

    #[derive(Debug, Clone)]
    struct TestCase {
        name: String,
        data: &'static str,
        opts: Options,
        exp_debug: Vec<DebugStep>,
        partition_exist: HashMap<i64, ()>,
        exp: FunnelResult,
    }

    #[traced_test]
    #[test]
    fn test_cases() -> anyhow::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("event", DataType::Utf8, false),
            Field::new("const", DataType::Int64, false),
        ])) as SchemaRef;

        let cases = vec![TestCase {
            name: "3 steps in a row should pass".to_string(),
            data: r#"
| user_id(i64) | ts(ts) | event(utf8) | const(i64) |
|--------------|--------|-------------|------------|
| 1            | 0      | e1          | 1          |
| 1            | 1      | e2          | 1          |
| 1            | 2      | e3          | 1          |
"#,

            opts: Options {
                ts_col: Column::new("ts", 1),
                window: Duration::seconds(15),
                steps: event_eq!(schema, "e1" Sequential, "e2" Sequential, "e3" Sequential),
                exclude: None,
                constants: None,
                count: Count::Unique,
                filter: None,
                touch: Touch::First,
                partition_col: Column::new("user_id", 0),
            },
            exp_debug: expected_debug!(Step NextRow Step NextRow Step),
            partition_exist: HashMap::from([(1, ())]),
            exp: FunnelResult {
                total_funnels: 1,
                completed_funnels: 1,
                steps: vec![
                    StepResult {
                        count: 1,
                        total_time: 0,
                        total_time_from_start: 0,
                    },
                    StepResult {
                        count: 1,
                        total_time: 1,
                        total_time_from_start: 1,
                    },
                    StepResult {
                        count: 1,
                        total_time: 1,
                        total_time_from_start: 2,
                    },
                ],
            },
        }];

        let run_only: Option<&str> = None;
        for case in cases.iter().cloned() {
            if let Some(name) = run_only {
                if case.name != name {
                    continue;
                }
            }
            println!("\ntest case : {}", case.name);
            println!("============================================================");
            let rbs = parse_markdown_tables(case.data).unwrap();

            let mut f = Funnel::new(case.opts);

            for rb in rbs {
                f.evaluate(&rb, &case.partition_exist)?;
            }
            f.finalize()?;
            assert_eq!(f.debug, case.exp_debug);
            assert_eq!(f.result, case.exp);
            println!("PASSED");
        }

        Ok(())
    }
}
