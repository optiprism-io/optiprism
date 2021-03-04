use std::marker::PhantomData;
use super::node::{NodeState, Node, EvalResult};
use super::context::Context;
use super::cmp::Cmp;
pub struct ScalarValue<T, C> {
    c: PhantomData<C>,
    state: NodeState,
    is_partition: bool,
    left: T,
    right: T,
}

impl<T, C> ScalarValue<T, C> {
    pub fn new(left: T, right: T) -> Self {
        ScalarValue {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left,
            right,
        }
    }

    pub fn new_partitioned(left: T, right: T) -> Self {
        let mut ret = ScalarValue::new(left, right);
        ret.is_partition = true;
        ret
    }
}

impl<T, C> Node for ScalarValue<T, C> where T: Copy, C: Cmp<T> {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        if C::is_true(self.left, self.right) {
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
    fn scalar_value_equal_fails() {
        let mut n = ScalarValue::<_,Equal>::new(2u32, 1u32);
        let ctx = Context::default();
        assert_eq!(n.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn scalar_value_equal() {
        let mut n = ScalarValue::<_,Equal>::new(1u32, 1u32);
        let ctx = Context::default();
        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn scalar_value_equal_fails_stateful() {
        let mut n = ScalarValue::<_,Equal>::new_partitioned(2, 1);
        let ctx = Context::default();
        assert_eq!(n.evaluate(&ctx), EvalResult::False(true))
    }


    #[test]
    fn scalar_value_stateful() {
        let mut n = ScalarValue::<_,Equal>::new_partitioned(1,1);
        let ctx = Context::default();
        assert_eq!(n.evaluate(&ctx), EvalResult::True(true))
    }
}