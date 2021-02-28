use crate::expression::context::Context;

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum EvalResult {
    True(bool),
    False(bool),
    ResetNode,
}

#[derive(PartialEq, Debug)]
pub enum NodeState {
    None,
    True,
    False,
}

pub trait Node {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult;
    fn reset(&mut self);
}