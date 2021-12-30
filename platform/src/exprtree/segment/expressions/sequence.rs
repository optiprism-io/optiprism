use crate::exprtree::segment::expressions::expr::Expr;
use crate::exprtree::segment::expressions::utils::into_array;
use arrow::array;
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, TimestampSecondArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

pub struct Sequence {
    timestamp_col_id: usize,
    window: Duration,
    steps: Vec<Arc<dyn PhysicalExpr>>,
    exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<StepID>)>>,
    constants: Option<Vec<ColID>>,
    filter: Option<Filter>,
}

type ColID = usize;
type StepID = usize;

// TODO: add drop off from step and time-to-convert
impl Sequence {
    pub fn new(
        timestamp_col_id: usize,
        window: Duration,
        steps: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            steps,
            timestamp_col_id,
            window,
            exclude: None,
            constants: None,
            filter: None,
        }
    }

    pub fn with_exclude(self, exclude: Vec<(Arc<dyn PhysicalExpr>, Vec<StepID>)>) -> Self {
        Sequence {
            timestamp_col_id: self.timestamp_col_id,
            window: self.window,
            steps: self.steps,
            exclude: Some(exclude),
            constants: self.constants,
            filter: None,
        }
    }

    pub fn with_constants(self, constants: Vec<ColID>) -> Self {
        Sequence {
            timestamp_col_id: self.timestamp_col_id,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: Some(constants),
            filter: None,
        }
    }

    pub fn with_drop_off_on_any_step(self) -> Self {
        Sequence {
            timestamp_col_id: self.timestamp_col_id,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::DropOffOnAnyStep),
        }
    }

    pub fn with_drop_off_on_step(self, drop_off_step_id: usize) -> Self {
        Sequence {
            timestamp_col_id: self.timestamp_col_id,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::DropOffOnStep(drop_off_step_id)),
        }
    }

    pub fn with_time_to_convert(self, from: Duration, to: Duration) -> Self {
        Sequence {
            timestamp_col_id: self.timestamp_col_id,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::TimeToConvert(from, to)),
        }
    }
}

fn check_constants(
    batch: &RecordBatch,
    row_id: usize,
    constants: &Vec<usize>,
    const_row_id: usize,
) -> bool {
    for col_id in constants.iter() {
        let col = &batch.columns()[*col_id];

        match (col.is_null(const_row_id), col.is_null(row_id)) {
            (true, true) => continue,
            (true, false) | (false, true) => return false,
            _ => {}
        }

        match col.data_type() {
            DataType::Int8 => {
                let left = col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(const_row_id);
                let right = col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(row_id);
                if left != right {
                    return false;
                }
            }
            _ => {
                unimplemented!()
            }
        }
    }

    return true;
}

impl Expr<bool> for Sequence {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        // make boolean array on each step
        let pre_steps: Vec<Arc<dyn Array>> = self
            .steps
            .iter()
            .map(|x| {
                // evaluate step
                into_array(x.evaluate(batch).unwrap())
            })
            .collect();

        let mut steps: Vec<&BooleanArray> = Vec::with_capacity(self.steps.len());
        for v in pre_steps.iter() {
            steps.push(v.as_any().downcast_ref::<BooleanArray>().unwrap())
        }

        // each step has 0 or more exclusion expressions
        let mut exclude: Vec<Vec<&BooleanArray>> = vec![Vec::new(); steps.len()];
        let mut pre_exclude: Vec<(Arc<dyn Array>, &Vec<usize>)> = Vec::new();

        // make exclude steps
        if let Some(e) = &self.exclude {
            for (expr, steps) in e.iter() {
                // calculate exclusion expression for step
                pre_exclude.push((into_array(expr.evaluate(batch).unwrap()), steps));
            }

            for (arr, steps) in pre_exclude.iter() {
                for step_id in *steps {
                    exclude[*step_id].push(arr.as_any().downcast_ref::<BooleanArray>().unwrap())
                }
            }
        }

        // downcast timestamp column
        let ts_col = batch.columns()[self.timestamp_col_id]
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();

        // current step id
        let mut step_id: usize = 0;
        // current row id
        let mut row_id: usize = 0;
        // reference on row_id of each step
        let mut step_row_id: Vec<usize> = vec![0; steps.len()];
        // timestamp of first step
        let mut window_start_ts: i64 = 0;
        let mut is_completed = false;

        while row_id < batch.num_rows() {
            // check window
            if step_id > 0 {
                let cur_ts = ts_col.value(row_id);
                // calculate window
                if cur_ts - window_start_ts > self.window.num_seconds() {
                    let mut found = false;
                    // search next first step occurrence
                    for i in (step_row_id[0] + 1)..batch.num_rows() {
                        if steps[0].value(i) {
                            step_id = 0;
                            // rewind to next step 0
                            row_id = i;
                            found = true;
                            break;
                        }
                    }

                    // break if no more first steps
                    if !found {
                        break;
                    }
                }
            }

            // check expression result
            if steps[step_id].value(row_id) {
                // check constants. We do not check constants on first step
                if step_id > 0 {
                    if let Some(constants) = &self.constants {
                        if !check_constants(batch, row_id, constants, step_row_id[0]) {
                            // skip current value if constant doesn't match
                            row_id += 1;
                            continue;
                        }
                    }
                }

                // save reference to current step row_id
                step_row_id[step_id] = row_id;
                // save window start if current step is first
                if step_id == 0 {
                    window_start_ts = ts_col.value(row_id);
                }

                if step_id == steps.len() - 1 {
                    is_completed = true;
                    break;
                }
                // move to next step
                step_id += 1;

                // increment current row
                row_id += 1;
                continue;
            }

            // check exclude
            // perf: use just regular loop with index, do not spawn exclude[step_id].iter() each time
            if step_id > 0 {
                for i in 0..exclude[step_id].len() {
                    if exclude[step_id][i].value(row_id) {
                        step_id -= 1;

                        break;
                    }
                }
            }

            row_id += 1;
        }

        // check filter
        if let Some(filter) = &self.filter {
            return match filter {
                Filter::DropOffOnAnyStep => !is_completed,
                Filter::DropOffOnStep(drop_off_step_id) => {
                    if is_completed {
                        return false;
                    }

                    step_id == *drop_off_step_id
                }
                Filter::TimeToConvert(from, to) => {
                    let spent = ts_col.value(row_id) - window_start_ts;
                    spent >= from.num_seconds() && spent <= to.num_seconds()
                }
            };
        }

        is_completed
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::expr::Expr;
    use crate::exprtree::segment::expressions::sequence::Sequence;
    use arrow::array::{Int8Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::error::Result;
    use datafusion::logical_plan::Operator;
    use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    fn build_table(
        a: (&str, &Vec<i8>),
        b: (&str, &Vec<i8>),
        c: (&str, &Vec<i8>),
        ts: (&str, &Vec<i64>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int8, false),
            Field::new(b.0, DataType::Int8, false),
            Field::new(c.0, DataType::Int8, false),
            Field::new(c.0, DataType::Timestamp(TimeUnit::Second, None), false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int8Array::from(a.1.clone())),
                Arc::new(Int8Array::from(b.1.clone())),
                Arc::new(Int8Array::from(c.1.clone())),
                Arc::new(TimestampSecondArray::from(ts.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_steps(schema: &Schema) -> (Arc<BinaryExpr>, Arc<BinaryExpr>, Arc<BinaryExpr>) {
        let step1 = {
            let left = Column::new_with_schema("a", schema).unwrap();
            let right = Literal::new(ScalarValue::Int8(Some(1)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step2 = {
            let left = Column::new_with_schema("a", schema).unwrap();
            let right = Literal::new(ScalarValue::Int8(Some(2)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step3 = {
            let left = Column::new_with_schema("a", schema).unwrap();
            let right = Literal::new(ScalarValue::Int8(Some(3)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };
        (Arc::new(step1), Arc::new(step2), Arc::new(step3))
    }

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        );
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_exclude() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![7, 1, 5, 4, 2, 5, 3, 1, 2, 4, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
        ]));

        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        {
            let exclude1 = {
                let left = Column::new_with_schema("a", &schema).unwrap();
                let right = Literal::new(ScalarValue::Int8(Some(4)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude2 = {
                let left = Column::new_with_schema("a", &schema).unwrap();
                let right = Literal::new(ScalarValue::Int8(Some(5)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude: Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)> = vec![
                (Arc::new(exclude1), vec![1]),
                (Arc::new(exclude2), vec![1, 2]),
            ];
            let op = Sequence::new(
                1,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_exclude(exclude);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let exclude1 = {
                let left = Column::new_with_schema("a", &schema).unwrap();
                let right = Literal::new(ScalarValue::Int8(Some(4)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude2 = {
                let left = Column::new_with_schema("a", &schema).unwrap();
                let right = Literal::new(ScalarValue::Int8(Some(5)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude: Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)> = vec![
                (Arc::new(exclude1), vec![2]),
                (Arc::new(exclude2), vec![1, 2]),
            ];
            let op = Sequence::new(
                1,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_exclude(exclude);
            assert_eq!(false, op.evaluate(&batch, 0));
        }
        Ok(())
    }

    #[test]
    fn test_constants() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]);

        let (step1, step2, step3) = build_steps(&schema);

        {
            let batch = build_table(
                ("a", &vec![1, 2, 3]),
                ("b", &vec![1, 1, 1]),
                ("c", &vec![2, 2, 2]),
                ("ts", &vec![0, 1, 2]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 2, 3]),
                ("b", &vec![2, 1, 1]),
                ("c", &vec![2, 2, 2]),
                ("ts", &vec![0, 1, 2]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 1, 2, 3]),
                ("b", &vec![1, 2, 2, 2]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 2, 1, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 1, 1, 1]),
                ("c", &vec![2, 1, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 1, 2, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 2, 2, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec![1, 2];
            let op = Sequence::new(
                3,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )
            .with_constants(constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        Ok(())
    }

    #[test]
    fn test_short_window() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(1),
            vec![step1.clone(), step2.clone(), step3.clone()],
        );
        assert_eq!(false, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_window_slide() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 4, 5, 1, 4, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3, 4, 5, 6]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(4),
            vec![step1.clone(), step2.clone(), step3.clone()],
        );
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_drop_off_on_step1() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(1),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_drop_off_on_step(2);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_drop_off_on_step2() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 4, 5]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_drop_off_on_step(2);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_drop_off_on_step3() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 3, 4, 5]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_drop_off_on_step(1);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_drop_off_on_step4() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![6, 3, 4, 5]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_drop_off_on_step(0);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_drop_off_on_any_step() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 3, 4, 5]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_drop_off_on_any_step();
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_with_time_to_convert() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 2]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_time_to_convert(Duration::seconds(1), Duration::seconds(2));
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    #[test]
    fn test_with_time_to_convert2() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let ts = Arc::new(TimestampSecondArray::from(vec![0, 1, 20]));
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone(), ts.clone()])?;

        let (step1, step2, step3) = build_steps(&schema);

        let op = Sequence::new(
            1,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )
        .with_time_to_convert(Duration::seconds(1), Duration::seconds(2));
        assert_eq!(false, op.evaluate(&batch, 0));
        Ok(())
    }
}
