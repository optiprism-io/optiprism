use super::node::{NodeState, EvalResult, Node};
use std::marker::PhantomData;
use super::context::Context;
use super::cmp::{Cmp, Equal};

pub struct VectorValue<T, C = Equal> {
    c: PhantomData<C>,
    state: NodeState,
    is_partition: bool,
    left: Vec<T>,
    right: T,
}

impl<T, C> VectorValue<T, C> {
    pub fn new(left: Vec<T>, right: T) -> Self {
        VectorValue {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left,
            right,
        }
    }

    pub fn new_partitioned(left: Vec<T>, right: T) -> Self {
        let mut ret = VectorValue::new(left, right);
        ret.is_partition = true;
        ret
    }
}

impl<T, C> Node for VectorValue<T, C> where T: Copy, C: Cmp<T> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };
        if C::is_true(self.left[ctx.row_id], self.right) {
            if self.is_partition {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_partition {
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
    use super::cmp::Equal;

    #[test]
    fn vector_value_equal_fails() {
        let mut n = VectorValue::<_,Equal>::new(vec![0u64, 1], 1u64);
        let ctx = Context { row_id: 0 };
        assert_eq!(n.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn vector_value_equal() {
        let mut n = VectorValue::<u32, Equal> {
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
        let mut n = VectorValue::<u32, Equal> {
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
        let mut n = VectorValue::<u32, Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 1 };
        assert_eq!(n.evaluate(&ctx), EvalResult::True(true))
    }
}