use super::context::Context;

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum EvalResult {
    True(bool),
    False(bool),
    ResetNode,
}

#[derive(PartialEq, Debug)]
pub enum ExprState {
    None,
    True,
    False,
}

pub trait Expr {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult;
    fn reset(&mut self);
}