use crate::operators::expression_tree::expr::{Expr, Node, EvalResult};
use crate::operators::expression_tree::context::Context;

struct BinaryOp<'a> {
    left: &'a dyn Expr<bool>,
    right: &'a dyn Expr<bool>,
}

impl<'a> Expr<bool> for BinaryOp<'a> {
    fn evaluate(&mut self, ctx: &Context) -> bool {
        unimplemented!()
    }

    fn reset(&mut self) {
        unimplemented!()
    }
}