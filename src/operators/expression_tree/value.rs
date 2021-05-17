use super::node::{NodeState, EvalResult, Node};
use std::marker::PhantomData;
use super::context::Context;
use super::cmp::{Cmp, Equal};
use arrow::array::{ArrayRef, Array};
use arrow::datatypes::SchemaRef;
use datafusion::{
    error::{Result},
};
use datafusion::scalar::ScalarValue;

pub use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;

pub struct Value<L, C, R> {
    c: PhantomData<C>,
    l: PhantomData<L>,
    state: NodeState,
    is_stateful: bool,
    left_col: usize,
    right_value: Option<R>,
}

impl<L, C, R> Value<L, C, R> {
    pub fn try_new(schema: SchemaRef, left_col_name: &str, right_value: Option<R>, is_stateful: bool) -> Result<Self> {
        Ok(Value {
            c: PhantomData,
            l: PhantomData,
            state: NodeState::None,
            is_stateful,
            left_col: schema.index_of(left_col_name)?,
            right_value,
        })
    }
}

impl<L: Array, C, R> Node for Value<L, C, R> where C: Cmp<L> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let col = ctx.batch.columns()[self.left_col].as_ref();
        if col.is_null(ctx.row_id) {
            match self.right_value {
                None => {
                    if self.is_stateful {
                        self.state = NodeState::True;
                        return EvalResult::True(true);
                    }
                }
                Some(_) => {
                    if self.is_stateful {
                        self.state = NodeState::False;
                        return EvalResult::False(true);
                    }
                }
            }
        }
        if C::is_true(ctx.row_id, col, self.right) {
            if self.is_stateful {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_stateful {
            self.state = NodeState::False;
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::cmp::Equal;
    use arrow::array::Int8Array;
    use std::sync::Arc;
    use arrow::datatypes::{Schema, Field, DataType};
    use arrow::record_batch::RecordBatch;

    #[test]
    fn test_true() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let mut op = Value::try_new(schema.clone(), "c", Some(ScalarValue::Int8(Some(1))), true)?;
        let v = Int8Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )?;

        let ctx = Context {
            batch,
            row_id: 1,
        };

        assert_eq!(op.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn test_true() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let mut op = Value::try_new(schema.clone(), "c", Some(ScalarValue::Int8(Some(1))), true)?;
        let v = Int8Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int8Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )?;

        let ctx = Context {
            batch,
            row_id: 1,
        };

        assert_eq!(op.evaluate(&ctx), EvalResult::True(true));
    }

    /*
    #[test]
    fn vector_value_equal_fails() {
        let mut n = Value::<_, Equal>::new(vec![0u64, 1], 1u64);
        let ctx = Context { row_id: 0 };
        assert_eq!(n.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn vector_value_equal() {
        let mut n = Value::<u32, Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 1 };
        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn vector_value_equal_fails_stateful() {
        let mut n = Value::<u32, Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 0 };
        assert_eq!(n.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn vector_value_equal_stateful() {
        let mut n = Value::<u32, Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 1 };
        assert_eq!(n.evaluate(&ctx), EvalResult::True(true))
    }*/
}