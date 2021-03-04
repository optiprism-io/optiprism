use super::node::{NodeState, Node, EvalResult};
use super::cmp::CmpValue;
use super::context::Context;

pub struct TrueCountLimit<'a> {
    state: NodeState,
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
    node: &'a mut dyn Node,
    is_grouped: bool,
}

impl<'a> TrueCountLimit<'a> {
    pub fn new(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        TrueCountLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: false,
        }
    }

    pub fn new_grouped(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        TrueCountLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: true,
        }
    }
}

impl<'a> Node for TrueCountLimit<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let result = self.node.evaluate(ctx);
        match result {
            EvalResult::True(true) => {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            EvalResult::True(false) => {
                self.count += 1;
            }
            EvalResult::False(true) => {
                self.state = NodeState::False;
                return EvalResult::False(true);
            }
            EvalResult::ResetNode => {
                if self.is_grouped {
                    return EvalResult::ResetNode;
                }
                self.reset();
                return EvalResult::False(false);
            }
            _ => {}
        };

        let from = self.from.cmp(self.count);
        let to = self.to.cmp(self.count);
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

    fn reset(&mut self) {
        self.count = 0;
        self.state = NodeState::None;
        self.node.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_value::TrueValue;

    #[test]
    fn true_count_limit() {
        let mut node = TrueValue::new();
        let mut limit = TrueCountLimit::new_grouped(&mut node, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);

        limit.reset();

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
    }
}