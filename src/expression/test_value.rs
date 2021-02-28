use crate::expression::node::{Node, EvalResult};
use crate::expression::context::Context;

pub struct TrueValue {
    is_partition: bool,
}

impl TrueValue {
    pub fn new() -> Self {
        TrueValue { is_partition: false }
    }

    pub fn new_partitioned() -> Self {
        TrueValue { is_partition: true }
    }
}

impl Node for TrueValue {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        EvalResult::True(self.is_partition)
    }

    fn reset(&mut self) {}
}

pub struct FalseValue {
    is_partition: bool,
}

impl FalseValue {
    pub fn new() -> Self {
        FalseValue { is_partition: false }
    }

    pub fn new_partitioned() -> Self {
        FalseValue { is_partition: true }
    }
}

impl Node for FalseValue {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        EvalResult::False(self.is_partition)
    }

    fn reset(&mut self) {}
}