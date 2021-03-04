use crate::expression::node::{NodeState, Node, EvalResult};
use crate::expression::context::Context;

pub struct Or<'a> {
    state: NodeState,
    nodes: Vec<&'a mut dyn Node>,
    is_grouped: bool,
}

impl<'a> Or<'a> {
    pub fn new(nodes: Vec<&'a mut dyn Node>) -> Self {
        Or {
            state: NodeState::None,
            nodes,
            is_grouped: false,
        }
    }

    pub fn new_grouped(nodes: Vec<&'a mut dyn Node>) -> Self {
        Or {
            state: NodeState::None,
            nodes,
            is_grouped: true,
        }
    }
}

impl<'a> Node for Or<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let mut is_stateful: bool = true;
        for c in self.nodes.iter_mut() {
            match c.evaluate(ctx) {
                EvalResult::True(stateful) => {
                    if stateful {
                        self.state = NodeState::True;
                    }
                    return EvalResult::True(stateful);
                }
                EvalResult::False(stateful) => {
                    if !stateful {
                        is_stateful = false;
                    }
                }
                EvalResult::ResetNode => {
                    if self.is_grouped {
                        return EvalResult::ResetNode;
                    }
                    self.reset();
                    return EvalResult::False(false);
                }
            }
        }
        EvalResult::False(is_stateful)
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
    use crate::expression::scalar_value::ScalarValue;
    use crate::expression::cmp::Equal;
    use std::marker::PhantomData;
    use crate::expression::test_value::{FalseValue, TrueValue};

    #[test]
    fn a_or_b() {
        let mut a = FalseValue::new();
        let mut b = TrueValue::new();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_b_fails() {
        let mut a = FalseValue::new();
        let mut b = TrueValue::new();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_stateful_b() {
        let mut a = FalseValue::new_partitioned();
        let mut b = TrueValue::new();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_stateful_b_stateful() {
        let mut a = FalseValue::new_partitioned();
        let mut b = TrueValue::new_partitioned();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn a_or_stateful_b_fails() {
        let mut a = FalseValue::new();
        let mut b = FalseValue::new_partitioned();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn stateful_a_or_stateful_b_fails() {
        let mut a = FalseValue::new_partitioned();
        let mut b = FalseValue::new_partitioned();
        let mut q = Or::new(vec![&mut a, &mut b]);
        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }
}