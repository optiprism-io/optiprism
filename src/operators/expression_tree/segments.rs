use crate::operators::expression_tree::expr::{Node, EvalResult};
use crate::operators::expression_tree::context::Context;

pub struct Segments<'a> {
    segments: Vec<&'a dyn Node>
}

impl<'a> Segments<'a> {
    pub fn new(segments: Vec<&'a dyn Node>) -> Self {
        Segments {
            segments,
        }
    }
}

impl<'a> Node for Segments<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        for (segment, id) in self.segments.iter_mut() {
            match segment.evaluate() {
                EvalResult::True(is_stateful) => {}
                EvalResult::False(is_stateful) => {}
                EvalResult::ResetNode => {}
            }
        }

        EvalResult::ResetNode
    }

    fn reset(&mut self) {}
}