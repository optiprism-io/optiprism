use crate::expression::node::{Node, EvalResult};
use crate::expression::context::Context;

pub struct AndNot<'a> {
    not_node: &'a mut dyn Node,
    node: &'a mut dyn Node,
}

impl<'a> AndNot<'a> {
    pub fn new(node: &'a mut dyn Node, not_node: &'a mut dyn Node) -> Self {
        AndNot {
            node,
            not_node,
        }
    }
}

impl<'a> Node for AndNot<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        if let EvalResult::False(_) = self.not_node.evaluate(ctx) {
            return EvalResult::ResetNode;
        }

        self.node.evaluate(ctx)
    }

    fn reset(&mut self) {
        self.not_node.reset();
        self.node.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::test_value::{TrueValue, FalseValue};

    #[test]
    fn and_not_false() {
        let mut a = TrueValue::new();
        let mut b = FalseValue::new();
        let mut q = AndNot::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::ResetNode);
    }

    #[test]
    fn and_not_true() {
        let mut a = TrueValue::new();
        let mut b = TrueValue::new();
        let mut q = AndNot::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
    }
}