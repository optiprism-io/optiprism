use super::expr::{ExprState, EvalResult, Expr};
use std::marker::PhantomData;
use super::context::Context;
use super::cmp::{Cmp, Equal};
use arrow::array::{ArrayRef, Array, Int8Array};
use arrow::datatypes::{SchemaRef, DataType};
use datafusion::{
    error::{Result},
};
use datafusion::scalar::ScalarValue;

pub use arrow::array::PrimitiveArray;
use arrow::datatypes::ArrowPrimitiveType;
use std::ops::Deref;
use std::any::Any;

pub struct Predicate<T, Op> {
    c: PhantomData<Op>,
    state: ExprState,
    is_stateful: bool,
    left_col: usize,
    right_value: Option<T>,
}

impl<T, Op> Predicate<T, Op> where Op: Cmp<T> {
    pub fn try_new(schema: SchemaRef, left_col_name: &str, right_value: Option<T>, is_stateful: bool) -> Result<Self> {
        Ok(Predicate {
            c: PhantomData,
            state: ExprState::None,
            is_stateful,
            left_col: schema.index_of(left_col_name)?,
            right_value,
        })
    }
}

impl<T: Copy, Op> Expr for Predicate<T, Op> where Op: Cmp<T> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            ExprState::True => return EvalResult::True(true),
            ExprState::False => return EvalResult::False(true),
            _ => {}
        };

        let col = &ctx.batch.columns()[self.left_col];

        if col.is_null(ctx.row_id) {
            match self.right_value {
                None => {
                    if self.is_stateful {
                        self.state = ExprState::True;
                        return EvalResult::True(true);
                    }
                    return EvalResult::True(false);
                }
                Some(_) => {
                    if self.is_stateful {
                        self.state = ExprState::False;
                        return EvalResult::False(true);
                    }
                    return EvalResult::False(false);
                }
            }
        } else if let None = self.right_value {
            if self.is_stateful {
                self.state = ExprState::False;
                return EvalResult::False(true);
            }
            return EvalResult::False(false);
        }

        if Op::is_true(ctx.row_id, col, self.right_value.unwrap()) {
            if self.is_stateful {
                self.state = ExprState::True;
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_stateful {
            self.state = ExprState::False;
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = ExprState::None;
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
    fn test_stateless_value() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let mut op = Predicate::<_, Equal>::try_new(schema.clone(), "c", Some(1i8), false)?;
        let mut ctx = Context {
            batch,
            row_id: 0,
        };

        assert_eq!(op.evaluate(&ctx), EvalResult::True(false));
        ctx.row_id = 1;
        assert_eq!(op.evaluate(&ctx), EvalResult::False(false));

        ctx.row_id = 0;

        let mut null_op = Predicate::<_, Equal>::try_new(schema.clone(), "c", None, false)?;

        ctx.row_id = 0;
        assert_eq!(null_op.evaluate(&ctx), EvalResult::False(false));
        ctx.row_id = 2;
        assert_eq!(null_op.evaluate(&ctx), EvalResult::True(false));

        Ok(())
    }

    #[test]
    fn test_stateful_value() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int8, true),
        ]));

        let c = Arc::new(Int8Array::from(vec![Some(1), Some(2), None]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                c.clone(),
            ],
        )?;

        let mut op = Predicate::<_, Equal>::try_new(schema.clone(), "c", Some(1i8), true)?;
        let mut ctx = Context {
            batch,
            row_id: 0,
        };

        assert_eq!(op.evaluate(&ctx), EvalResult::True(true));
        ctx.row_id = 1;
        assert_eq!(op.evaluate(&ctx), EvalResult::True(true));

        Ok(())
    }
}