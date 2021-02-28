use crate::expression::node::{Node, NodeState, EvalResult};
use crate::expression::context::Context;

pub struct And<'a> {
    state: NodeState,
    nodes: Vec<&'a mut dyn Node>,
}

impl<'a> And<'a> {
    pub fn new(nodes: Vec<&'a mut dyn Node>) -> Self {
        And {
            state: NodeState::None,
            nodes,
        }
    }
}

impl<'a> Node for And<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        // node is stateful by default
        let mut is_stateful: bool = true;
        for c in self.nodes.iter_mut() {
            match c.evaluate(ctx) {
                EvalResult::True(stateful) => {
                    // node is stateful only if all evaluations are true and stateful
                    if !stateful {
                        is_stateful = false
                    }
                }
                EvalResult::False(stateful) => {
                    // if some failed nodes is stateful, then it make current node failed stateful  as well
                    if stateful {
                        self.state = NodeState::False;
                    }
                    return EvalResult::False(stateful);
                }
                EvalResult::ResetNode => {
                    self.reset();
                    return EvalResult::False(false);
                }
            }
        }

        if is_stateful {
            self.state = NodeState::True;
            return EvalResult::True(true);
        }

        EvalResult::True(false)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        for c in self.nodes.iter_mut() {
            c.reset()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::marker::PhantomData;
    use crate::expression::test_value::{FalseValue, TrueValue};
    use crate::expression::vector_value::VectorValue;
    use crate::expression::cmp::Equal;

    #[test]
    fn a_and_b_fails() {
        let mut a = FalseValue::new();
        let mut b = TrueValue::new();
        let mut q = And::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn a_and_b() {
        let mut a = TrueValue::new();
        let mut b = TrueValue::new();
        let mut q = And::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_stateful_and_b() {
        let mut a = TrueValue::new_partitioned();
        let mut b = TrueValue::new();
        let mut q = And::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
    }

    #[test]
    fn a_stateful_and_b_stateful() {
        let mut a = VectorValue::<u32, Equal>::new_partitioned(vec![1, 2], 1);
        let mut b = VectorValue::<_, Equal>::new_partitioned(vec![2, 3], 2);
        let mut q = And::new(vec![&mut a, &mut b]);
        let mut ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
        ctx.row_id = 1;
        // check for stateful
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn a_stateful_and_b_stateful_fails() {
        let mut a = FalseValue::new_partitioned();
        let mut b = TrueValue::new_partitioned();
        let mut q = And::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }
}