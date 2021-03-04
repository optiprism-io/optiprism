use super::node::{Node, EvalResult};
use super::context::Context;

pub struct Sequence<'a> {
    left_node: &'a mut dyn Node,
    right_node: &'a mut dyn Node,
    state: SequenceState,
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum SequenceState {
    True,
    False,
    LeftNode,
    RightNode,
}


impl<'a> Sequence<'a> {
    fn new(left: &'a mut dyn Node, right: &'a mut dyn Node) -> Self {
        Sequence {
            left_node: left,
            right_node: right,
            state: SequenceState::LeftNode,
        }
    }
}

impl<'a> Node for Sequence<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        match self.state {
            SequenceState::True => {
                return EvalResult::True(true);
            }
            SequenceState::False => {
                return EvalResult::False(true);
            }
            SequenceState::LeftNode => {
                return match self.left_node.evaluate(ctx) {
                    EvalResult::True(_) => {
                        self.state = SequenceState::RightNode;
                        EvalResult::False(false)
                    }
                    EvalResult::False(true) => {
                        self.state = SequenceState::False;
                        EvalResult::False(true)
                    }
                    EvalResult::False(false) => EvalResult::False(false),
                    EvalResult::ResetNode => {
                        self.reset();
                        EvalResult::False(false)
                    }
                };
            }

            SequenceState::RightNode => {
                return match self.right_node.evaluate(ctx) {
                    EvalResult::True(_) => {
                        self.state = SequenceState::True;
                        EvalResult::True(true)
                    }
                    EvalResult::False(true) => {
                        self.state = SequenceState::False;
                        EvalResult::False(true)
                    }
                    EvalResult::False(false) => EvalResult::False(false),
                    EvalResult::ResetNode => {
                        self.reset();
                        EvalResult::False(false)
                    }
                };
            }
        }
    }

    fn reset(&mut self) {
        self.state = SequenceState::LeftNode;
        self.left_node.reset();
        self.right_node.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::scalar_value::ScalarValue;
    use super::cmp::{Equal, CmpValue};
    use std::marker::PhantomData;
    use super::node::NodeState;
    use super::test_value::{FalseValue, TrueValue};
    use super::true_count_limit::TrueCountLimit;
    use super::and::And;
    use super::and_not::AndNot;

    #[test]
    fn sequence() {
        let mut a = TrueValue::new();
        let mut b = TrueValue::new();
        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn sequence_right_fail() {
        let mut a = TrueValue::new();
        let mut b = FalseValue::new();
        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn sequence_left_stateful_fail() {
        let mut a = FalseValue::new_partitioned();
        let mut b = TrueValue::new();
        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
    }

    #[test]
    fn sequence_scenario() {
        let mut a = TrueValue::new();
        let mut b_fail = FalseValue::new();
        let mut b = TrueValue::new();

        let mut s = Sequence::new(&mut a, &mut b_fail);

        let ctx = Context::default();
        // pass first node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        // fail at second node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        // set second node to True
        s.right_node = &mut b;
        // pass second node
        assert_eq!(s.evaluate(&ctx), EvalResult::True(true));
        assert_eq!(s.state, SequenceState::True);
    }

    #[test]
    fn sequence_with_true_limit() {
        let mut a = TrueValue::new();
        let mut a_true_limit = TrueCountLimit::new(&mut a, CmpValue::GreaterEqual(3), CmpValue::LessEqual(3));
        let mut b = TrueValue::new();
        let mut b_true_limit = TrueCountLimit::new(&mut b, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut s = Sequence::new(&mut a_true_limit, &mut b_true_limit);
        let ctx = Context::default();

        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::LeftNode);
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::LeftNode);
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        assert_eq!(s.evaluate(&ctx), EvalResult::True(true));
        assert_eq!(s.state, SequenceState::True);
    }

    #[test]
    fn sequence_with_and_not() {
        let mut a = TrueValue::new();
        let mut b = TrueValue::new();
        let mut not = FalseValue::new();
        let mut and_not = AndNot::new(&mut b, &mut not);
        let mut s = Sequence::new(&mut a, &mut and_not);
        let ctx = Context::default();
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        // sequence reset
        assert_eq!(s.state, SequenceState::LeftNode);
    }
}