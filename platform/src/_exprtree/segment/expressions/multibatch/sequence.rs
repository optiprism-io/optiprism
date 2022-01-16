use crate::exprtree::segment::expressions::multibatch::expr::Expr;
use crate::exprtree::segment::expressions::utils::into_array;
use arrow::array;
use arrow::array::{Array, ArrayRef, BooleanArray, Int8Array, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::{ColumnarValue, PhysicalExpr};
use futures::{TryFutureExt, TryStreamExt};
use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, Neg};
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

#[derive(Clone)]
struct Row {
    batch_id: usize,
    row_id: usize,
}

#[derive(Debug)]
pub struct Sequence {
    schema: SchemaRef,
    ts_col: Column,
    window: Duration,
    steps: Vec<Arc<dyn PhysicalExpr>>,
    exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>>,
    // expr and vec of step ids
    constants: Option<Vec<usize>>,
    // vec of col ids
    filter: Option<Filter>,
}

impl Sequence {
    pub fn try_new(
        schema: SchemaRef,
        timestamp_col: Column,
        window: Duration,
        steps: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DatafusionResult<Self> {
        let (ts_field) = schema.field_with_name(timestamp_col.name())?;
        match ts_field.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {}
            other => {
                return Err(DataFusionError::Plan(format!(
                    "Timestamp column must be Timestamp, not {:?}",
                    other,
                )))
            }
        };

        if window.is_zero() || window.num_seconds() < 0 {
            return Err(DataFusionError::Plan(format!(
                "Invalid window {:?}",
                window
            )));
        }

        if steps.is_empty() {
            return Err(DataFusionError::Plan("Empty steps".to_string()));
        }
        for step in steps.iter() {
            match step.data_type(&schema)? {
                DataType::Boolean => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "Step expression must return boolean values, not {:?}",
                        other,
                    )))
                }
            }
        }
        Ok(Self {
            schema,
            steps,
            ts_col: timestamp_col.clone(),
            window,
            exclude: None,
            constants: None,
            filter: None,
        })
    }

    pub fn with_exclude(
        self,
        exclude: Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>,
    ) -> DatafusionResult<Self> {
        if exclude.is_empty() {
            return Err(DataFusionError::Plan("Empty exclude".to_string()));
        }

        for (expr, steps) in exclude.iter() {
            let dt = expr.data_type(&self.schema);
            if dt.is_err() {
                return Err(DataFusionError::Internal("Invalid data type".to_string()));
            }
            match expr.data_type(&self.schema) {
                Ok(t) => match t {
                    DataType::Boolean => {}
                    other => {
                        return Err(DataFusionError::Plan(format!(
                            "Exclude expression must return boolean values, not {:?}",
                            other,
                        )))
                    }
                },
                Err(e) => return Err(e),
            }

            if steps.is_empty() {
                return Err(DataFusionError::Plan("Empty steps in exclude".to_string()));
            }
        }

        for (expr, steps) in exclude.iter() {
            match expr.data_type(&self.schema)? {
                DataType::Boolean => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "Exclude expression must return boolean values, not {:?}",
                        other,
                    )))
                }
            }
            if steps.is_empty() {
                return Err(DataFusionError::Plan("Empty steps in exclude".to_string()));
            }
            for step_id in steps.iter() {
                if *step_id > self.steps.len() - 1 {
                    return Err(DataFusionError::Plan("Invalid step".to_string()));
                }
            }
        }
        Ok(Sequence {
            schema: self.schema,
            ts_col: self.ts_col,
            window: self.window,
            steps: self.steps,
            exclude: Some(exclude),
            constants: self.constants,
            filter: None,
        })
    }

    pub fn with_constants(self, constants: Vec<&str>) -> DatafusionResult<Self> {
        if constants.is_empty() {
            return Err(DataFusionError::Plan("Empty constants".to_string()));
        }
        let c: Vec<usize> = constants
            .iter()
            .map(|x| {
                return match self.schema.column_with_name(*x) {
                    None => Err(DataFusionError::Plan(format!("Column {} not found", *x))),
                    Some((i, f)) => Ok(i),
                };
            })
            .collect::<DatafusionResult<Vec<usize>>>()?;

        Ok(Sequence {
            schema: self.schema,
            ts_col: self.ts_col,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: Some(c),
            filter: None,
        })
    }

    pub fn with_drop_off_on_any_step(self) -> Self {
        Sequence {
            schema: self.schema,
            ts_col: self.ts_col,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::DropOffOnAnyStep),
        }
    }

    pub fn with_drop_off_on_step(self, drop_off_step_id: usize) -> DatafusionResult<Self> {
        if drop_off_step_id > self.steps.len() - 1 {
            return Err(DataFusionError::Plan("Invalid step".to_string()));
        }
        Ok(Sequence {
            schema: self.schema,
            ts_col: self.ts_col,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::DropOffOnStep(drop_off_step_id)),
        })
    }

    pub fn with_time_to_convert(self, from: Duration, to: Duration) -> DatafusionResult<Self> {
        if from > to {
            return Err(DataFusionError::Internal("Invalid interval".to_string()));
        }
        Ok(Sequence {
            schema: self.schema,
            ts_col: self.ts_col,
            window: self.window,
            steps: self.steps,
            exclude: self.exclude,
            constants: self.constants,
            filter: Some(Filter::TimeToConvert(from, to)),
        })
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "Sequence")
    }
}

fn check_constants(
    batches: &[RecordBatch],
    row: &Row,
    constants: &Vec<usize>,
    const_row: &Row,
) -> DatafusionResult<bool> {
    for col_id in constants {
        let col = &batches[row.batch_id].columns()[*col_id];
        // current col and const col are the same, but they can be in different batches
        let const_col = &batches[const_row.batch_id].columns()[*col_id];

        match (col.is_null(row.row_id), const_col.is_null(const_row.row_id)) {
            (true, true) => continue,
            (true, false) | (false, true) => return Ok(false),
            _ => {}
        }

        match col.data_type() {
            DataType::Int8 => {
                let left = const_col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(const_row.row_id);
                let right = col
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .value(row.row_id);
                if left != right {
                    return Ok(false);
                }
            }
            other => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported format {:?}",
                    other
                )))
            }
        }
    }

    Ok(true)
}

fn inc_row(row: &mut Row, batches: &[RecordBatch]) -> bool {
    if row.row_id == batches[row.batch_id.clone()].num_rows() - 1 {
        if row.batch_id == batches.len() - 1 {
            return false;
        }
        row.batch_id += 1;
        row.row_id = 0;
    } else {
        row.row_id += 1;
    }

    true
}

impl Expr for Sequence {
    fn evaluate(&self, batches: &[RecordBatch]) -> DatafusionResult<bool> {
        let pre_steps = self
            .steps
            .iter()
            .map(|step| {
                batches
                    .iter()
                    .map(|batch| step.evaluate(batch).map(|v| into_array(v)))
                    .collect::<DatafusionResult<Vec<_>>>()
            })
            .collect::<DatafusionResult<Vec<Vec<ArrayRef>>>>()?;

        let steps = pre_steps
            .iter()
            .map(|x| {
                x.iter()
                    .map(|a| a.as_any().downcast_ref::<BooleanArray>().unwrap())
                    .collect()
            })
            .collect::<Vec<Vec<&BooleanArray>>>();

        // each step has 0 or more exclusion expressions
        let mut pre_exclude: Vec<(Vec<Arc<dyn Array>>, &Vec<usize>)> = Vec::new();
        let mut exclude: Vec<Vec<Vec<&BooleanArray>>> = vec![Vec::new(); steps.len()];

        // make exclude steps
        if let Some(e) = &self.exclude {
            for (expr, steps) in e.iter() {
                let b = batches
                    .iter()
                    .map(|batch| expr.evaluate(batch).map(|v| into_array(v)))
                    .collect::<DatafusionResult<Vec<ArrayRef>>>()?;
                // calculate exclusion expression for step
                pre_exclude.push((b, steps));
            }

            for (arrs, steps) in pre_exclude.iter() {
                for step_id in *steps {
                    let b = arrs
                        .iter()
                        .map(|x| x.as_any().downcast_ref::<BooleanArray>().unwrap())
                        .collect();
                    exclude[*step_id].push(b);
                }
            }
        }

        // downcast timestamp column

        let mut ts_col_tmp = batches
            .iter()
            .map(|batch| Ok(into_array(self.ts_col.evaluate(batch)?)))
            .collect::<DatafusionResult<Vec<Arc<dyn Array>>>>()?;

        let ts_col: Vec<Arc<&TimestampSecondArray>> = ts_col_tmp
            .iter()
            .map(|a| Arc::new(a.as_any().downcast_ref::<TimestampSecondArray>().unwrap()))
            .collect();

        // current step id
        let mut step_id: usize = 0;
        // current row id
        let mut row: Row = Row {
            batch_id: 0,
            row_id: 0,
        };
        // reference on row_id of each step
        let mut step_row: Vec<Row> = vec![
            Row {
                batch_id: 0,
                row_id: 0
            };
            steps.len()
        ];
        // timestamp of first step
        let mut window_start_ts: i64 = 0;
        let mut is_completed = false;

        loop {
            // check window
            if step_id > 0 {
                let cur_ts = ts_col[row.batch_id.clone()].value(row.row_id.clone());
                // calculate window
                if cur_ts - window_start_ts > self.window.num_seconds() {
                    let mut found = false;
                    let mut f_row = Row {
                        batch_id: step_row[0].batch_id,
                        row_id: step_row[0].row_id + 1,
                    };
                    // search next first step occurrence

                    loop {
                        if steps[f_row.batch_id.clone()][0].value(f_row.row_id.clone()) {
                            step_id = 0;
                            row = f_row.clone();
                            found = true;
                            break;
                        }
                        if !inc_row(&mut f_row, batches) {
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
            if steps[step_id][row.batch_id.clone()].value(row.row_id.clone()) {
                // check constants. We do not check constants on first step
                if step_id > 0 {
                    if let Some(constants) = &self.constants {
                        if !check_constants(batches, &row, constants, &step_row[0])? {
                            // skip current value if constant doesn't match
                            if !inc_row(&mut row, batches) {
                                break;
                            }
                            continue;
                        }
                    }
                }

                // save reference to current step
                step_row[step_id] = row.clone();
                // save window start if current step is first
                if step_id == 0 {
                    window_start_ts = ts_col[row.batch_id.clone()].value(row.row_id.clone());
                }

                if step_id == steps.len() - 1 {
                    is_completed = true;
                    break;
                }
                // move to next step
                step_id += 1;

                // increment current row
                if !inc_row(&mut row, batches) {
                    break;
                }
                continue;
            }

            // check exclude
            // perf: use just regular loop with index, do not spawn exclude[step_id].iter() each time
            if step_id > 0 {
                if !exclude.is_empty() {
                    for e in &exclude[step_id] {
                        if e[row.batch_id].value(row.row_id.clone()) {
                            step_id -= 1;
                            break;
                        }
                    }
                }
            }

            if !inc_row(&mut row, batches) {
                break;
            }
        }

        // check filter
        if let Some(filter) = &self.filter {
            return match filter {
                Filter::DropOffOnAnyStep => Ok(!is_completed),
                Filter::DropOffOnStep(drop_off_step_id) => {
                    if is_completed {
                        return Ok(false);
                    }

                    Ok(step_id == *drop_off_step_id)
                }
                Filter::TimeToConvert(from, to) => {
                    let spent = ts_col[row.batch_id.clone()].value(row.row_id) - window_start_ts;
                    Ok(spent >= from.num_seconds() && spent <= to.num_seconds())
                }
            };
        }

        Ok(is_completed)
    }
}

#[cfg(test)]
mod tests {
    use crate::exprtree::segment::expressions::multibatch::expr::Expr;
    use crate::exprtree::segment::expressions::multibatch::sequence::Sequence;
    use arrow::array::{Int8Array, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
    ) -> (SchemaRef, RecordBatch) {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int8, false),
            Field::new(b.0, DataType::Int8, false),
            Field::new(c.0, DataType::Int8, false),
            Field::new(ts.0, DataType::Timestamp(TimeUnit::Second, None), false),
        ]);

        (
            Arc::new(schema.clone()),
            RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![
                    Arc::new(Int8Array::from(a.1.clone())),
                    Arc::new(Int8Array::from(b.1.clone())),
                    Arc::new(Int8Array::from(c.1.clone())),
                    Arc::new(TimestampSecondArray::from(ts.1.clone())),
                ],
            )
            .unwrap(),
        )
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_exclude(exclude)?;
            assert_eq!(true, op.evaluate(vec![batch.clone()].as_slice())?);
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
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_exclude(exclude)?;
            assert_eq!(false, op.evaluate(vec![batch.clone()].as_slice())?);
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
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 3]),
                ("b", &vec![1, 1, 1]),
                ("c", &vec![2, 2, 2]),
                ("ts", &vec![0, 1, 2]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 3]),
                ("b", &vec![2, 1, 1]),
                ("c", &vec![2, 2, 2]),
                ("ts", &vec![0, 1, 2]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(false, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 1, 2, 3]),
                ("b", &vec![1, 2, 2, 2]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(false, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 2, 1, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 1, 1, 1]),
                ("c", &vec![2, 1, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 1, 2, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
        }

        {
            let (schema, batch) = build_table(
                ("a", &vec![1, 2, 2, 3]),
                ("b", &vec![1, 2, 2, 1]),
                ("c", &vec![2, 2, 2, 2]),
                ("ts", &vec![0, 1, 2, 3]),
            );
            let constants = vec!["b", "c"];
            let op = Sequence::try_new(
                schema.clone(),
                Column::new_with_schema("ts", &schema)?,
                Duration::seconds(100),
                vec![step1.clone(), step2.clone(), step3.clone()],
            )?
            .with_constants(constants)?;
            assert_eq!(false, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(1),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?;
        assert_eq!(false, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(4),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(1),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_drop_off_on_step(2)?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_drop_off_on_step(2)?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_drop_off_on_step(1)?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_drop_off_on_step(0)?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_drop_off_on_any_step();
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_time_to_convert(Duration::seconds(1), Duration::seconds(2))?;
        assert_eq!(true, op.evaluate(vec![batch].as_slice())?);
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

        let op = Sequence::try_new(
            schema.clone(),
            Column::new_with_schema("ts", &schema)?,
            Duration::seconds(100),
            vec![step1.clone(), step2.clone(), step3.clone()],
        )?
        .with_time_to_convert(Duration::seconds(1), Duration::seconds(2))?;
        assert_eq!(false, op.evaluate(vec![batch].as_slice())?);
        Ok(())
    }
}
