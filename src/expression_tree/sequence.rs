use crate::expression_tree::expr::Expr;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{PhysicalExpr, ColumnarValue};
use std::sync::Arc;
use arrow::array::{BooleanArray, Array, ArrayRef, Int8Array};
use std::borrow::Borrow;
use std::ops::Deref;
use arrow::array;
use arrow::datatypes::DataType;

pub struct Sequence {
    steps: Vec<Arc<dyn PhysicalExpr>>,
    exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>>,
    constants: Option<Vec<usize>>,
}

impl Sequence {
    pub fn new(steps: Vec<Arc<dyn PhysicalExpr>>, exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>>, constants: Option<Vec<usize>>) -> Self {
        Self {
            steps,
            exclude,
            constants,
        }
    }
}

fn check_constants(batch: &RecordBatch, row_id: usize, constants: &Vec<usize>, const_row_id: usize) -> bool {
    for col_id in constants.iter() {
        let col = &batch.columns()[*col_id];

        match (col.is_null(const_row_id), col.is_null(row_id)) {
            (true, true) => continue,
            (true, false) | (false, true) => return false,
            _ => {}
        }

        match col.data_type() {
            DataType::Int8 => {
                let left = col.as_any().downcast_ref::<Int8Array>().unwrap().value(const_row_id);
                let right = col.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id);
                if left != right {
                    return false;
                }
            }
            _ => { panic!("unimplemented") }
        }
    }

    return true;
}

impl Expr<bool> for Sequence {
    fn evaluate(&self, batch: &RecordBatch, _: usize) -> bool {
        let mut steps: Vec<&BooleanArray> = Vec::with_capacity(self.steps.len());

        let pre_steps: Vec<Arc<dyn Array>> = self.steps.iter().map(|x| {
            if let ColumnarValue::Array(v) = x.evaluate(batch).unwrap() {
                return v;
            };
            panic!("unexpected");
        }).collect();


        for v in pre_steps.iter() {
            steps.push(v.as_any().downcast_ref::<BooleanArray>().unwrap())
        }

        let mut exclude: Vec<Vec<&BooleanArray>> = vec![Vec::new(); steps.len()];
        let mut pre_exclude: Vec<(Arc<dyn Array>, &Vec<usize>)> = Vec::new();

        if let Some(e) = &self.exclude {
            for (expr, steps) in e.iter() {
                if let ColumnarValue::Array(a) = expr.evaluate(batch).unwrap() {
                    pre_exclude.push((a, steps));
                } else {
                    panic!("unexpected");
                }
            }

            for (arr, steps) in pre_exclude.iter() {
                for step_id in *steps {
                    exclude[*step_id].push(arr.as_any().downcast_ref::<BooleanArray>().unwrap())
                }
            }
        }

        let mut step_id: usize = 0;
        let mut row_id: usize = 0;
        let mut step_row_id: Vec<usize> = vec![0; steps.len()];

        while row_id < batch.num_rows() {
            if steps[step_id].value(row_id) {
                if step_id > 0 {
                    if let Some(constants) = &self.constants {
                        if !check_constants(batch, row_id, constants, step_row_id[0]) {
                            row_id += 1;
                            continue;
                        }
                    }
                }
                step_row_id[step_id] = row_id;
                step_id += 1;
                if step_id >= steps.len() {
                    return true;
                }
                row_id += 1;
                continue;
            }

            // perf: use just regular loop with index, do not spawn exclude[step_id].iter() each time
            for i in 0..exclude[step_id].len() {
                if exclude[step_id][i].value(row_id) {
                    if step_id > 0 {
                        step_id -= 1;
                    }

                    break;
                }
            }

            row_id += 1;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::array::Int8Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::expressions::{Column, Literal, BinaryExpr};
    use datafusion::scalar::ScalarValue;
    use datafusion::logical_plan::Operator;
    use datafusion::{
        error::{Result},
    };
    use crate::expression_tree::sequence::Sequence;
    use crate::expression_tree::expr::Expr;
    use datafusion::physical_plan::PhysicalExpr;

    /// returns a table with 3 columns of i32 in memory
    fn build_table_i8(
        a: (&str, &Vec<i8>),
        b: (&str, &Vec<i8>),
        c: (&str, &Vec<i8>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int8, false),
            Field::new(b.0, DataType::Int8, false),
            Field::new(c.0, DataType::Int8, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int8Array::from(a.1.clone())),
                Arc::new(Int8Array::from(b.1.clone())),
                Arc::new(Int8Array::from(c.1.clone())),
            ],
        )
            .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i8>),
        b: (&str, &Vec<i8>),
        c: (&str, &Vec<i8>),
    ) -> RecordBatch {
        return build_table_i8(a, b, c);
    }

    #[test]
    fn test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;

        let step1 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(1)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step2 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(2)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step3 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(3)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let op = Sequence::new(vec![Arc::new(step1), Arc::new(step2), Arc::new(step3)], None, None);
        assert_eq!(true, op.evaluate(&batch, 0));
        Ok(())
    }

    fn build_steps() -> (Arc<BinaryExpr>, Arc<BinaryExpr>, Arc<BinaryExpr>) {
        let step1 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(1)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step2 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(2)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };

        let step3 = {
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(3)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        };
        (Arc::new(step1), Arc::new(step2), Arc::new(step3))
    }

    #[test]
    fn test_exclude() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
        ]));

        let a = Arc::new(Int8Array::from(vec![7, 1, 5, 4, 2, 5, 3, 1, 2, 4, 3]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                a.clone(),
            ],
        )?;
        let step1 = Arc::new({
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(1)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        });

        let step2 = Arc::new({
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(2)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        });

        let step3 = Arc::new({
            let left = Column::new("a");
            let right = Literal::new(ScalarValue::Int8(Some(3)));
            BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
        });
        {
            let exclude1 = {
                let left = Column::new("a");
                let right = Literal::new(ScalarValue::Int8(Some(4)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude2 = {
                let left = Column::new("a");
                let right = Literal::new(ScalarValue::Int8(Some(5)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>> = Some(vec![(Arc::new(exclude1), vec![1]), (Arc::new(exclude2), vec![1, 2])]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], exclude, None);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let exclude1 = {
                let left = Column::new("a");
                let right = Literal::new(ScalarValue::Int8(Some(4)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude2 = {
                let left = Column::new("a");
                let right = Literal::new(ScalarValue::Int8(Some(5)));
                BinaryExpr::new(Arc::new(left), Operator::Eq, Arc::new(right))
            };

            let exclude: Option<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>> = Some(vec![(Arc::new(exclude1), vec![2]), (Arc::new(exclude2), vec![1, 2])]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], exclude, None);
            assert_eq!(false, op.evaluate(&batch, 0));
        }
        Ok(())
    }

    #[test]
    fn test_constants() -> Result<()> {
        let (step1, step2, step3) = build_steps();

        {
            let batch = build_table(("a", &vec![1, 2, 3]), ("b", &vec![1, 1, 1]), ("c", &vec![2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 2, 3]), ("b", &vec![2, 1, 1]), ("c", &vec![2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 1, 2, 3]), ("b", &vec![1, 2, 2, 2]), ("c", &vec![2, 2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 2, 2, 3]), ("b", &vec![1, 2, 1, 1]), ("c", &vec![2, 2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 2, 2, 3]), ("b", &vec![1, 1, 1, 1]), ("c", &vec![2, 1, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 2, 2, 3]), ("b", &vec![1, 1, 2, 1]), ("c", &vec![2, 2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(true, op.evaluate(&batch, 0));
        }

        {
            let batch = build_table(("a", &vec![1, 2, 2, 3]), ("b", &vec![1, 2, 2, 1]), ("c", &vec![2, 2, 2, 2]));
            let constants = Some(vec![1, 2]);
            let op = Sequence::new(vec![step1.clone(), step2.clone(), step3.clone()], None, constants);
            assert_eq!(false, op.evaluate(&batch, 0));
        }

        Ok(())
    }
}