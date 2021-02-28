use std::convert::TryFrom;
use crate::expression::node::{Node, NodeState, EvalResult};
use crate::expression::context::Context;
use crate::expression::cmp::CmpValue;

pub struct AbsoluteTimeWindowLimit<'a, T> {
    from: CmpValue<T>,
    to: CmpValue<T>,
    values: &'a [T],
    node: &'a mut dyn Node,
    state: NodeState,
    is_grouped: bool, // grouped limit just return ResetNode no parent, ungrouped limit resets all children
}

impl<'a, T: TryFrom<i64>> AbsoluteTimeWindowLimit<'a, T> {
    pub fn new(values: &'a [T], node: &'a mut dyn Node, from: CmpValue<i64>, to: CmpValue<i64>) -> Self {
        AbsoluteTimeWindowLimit {
            from: from.to(),
            to: to.to(),
            values,
            node,
            state: NodeState::None,
            is_grouped: false,
        }
    }

    pub fn new_grouped(values: &'a [T], node: &'a mut dyn Node, from: CmpValue<i64>, to: CmpValue<i64>) -> Self {
        AbsoluteTimeWindowLimit {
            from: from.to(),
            to: to.to(),
            values,
            node,
            state: NodeState::None,
            is_grouped: true,
        }
    }
}

impl<'a, T> Node for AbsoluteTimeWindowLimit<'a, T> where T: Copy + PartialEq + PartialOrd {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        let ts = self.values[ctx.row_id];

        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        // evaluate main node
        let result = self.node.evaluate(ctx);

        return match result {
            EvalResult::True(true) => {
                self.state = NodeState::True;
                EvalResult::True(true)
            }
            EvalResult::False(true) => {
                self.state = NodeState::False;
                EvalResult::False(true)
            }
            EvalResult::ResetNode => {
                if self.is_grouped {
                    // just return resetNode state no children
                    return EvalResult::ResetNode;
                }
                // reset itself and children
                self.reset();
                return EvalResult::False(false);
            }
            _ => {
                let from = self.from.cmp(ts);
                let to = self.to.cmp(ts);
                if from && to {
                    return result;
                }
                if self.to.is_set() && !to {
                    if self.is_grouped {
                        return EvalResult::ResetNode;
                    }
                    self.reset();
                    return EvalResult::False(false);
                }

                EvalResult::False(false)
            }
        };
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        self.node.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::test_value::TrueValue;
    use crate::expression::true_count_limit::TrueCountLimit;

    #[test]
    fn absolute_time_window_limit() {
        let mut node = TrueValue::new();
        let mut limit = AbsoluteTimeWindowLimit::new_grouped(
            &[0u32; 1],
            &mut node,
            CmpValue::GreaterEqual(1),
            CmpValue::LessEqual(2),
        );
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        limit.values = &[1u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        limit.values = &[2u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        limit.values = &[3u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
    }

    #[test]
    fn limits_chain() {
        let mut node = TrueValue::new();
        let mut true_count_limit = TrueCountLimit::new_grouped(&mut node, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut window_limit = AbsoluteTimeWindowLimit::new(&[0u32; 1], &mut true_count_limit, CmpValue::GreaterEqual(0), CmpValue::LessEqual(1));
        let mut ctx = Context { row_id: 0 };
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false)); // here is reset
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(window_limit.evaluate(&ctx), EvalResult::True(false));
    }
}