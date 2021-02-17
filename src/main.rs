use std::marker::PhantomData;

mod cmp;

#[derive(Default)]
struct Context {
    row_id: usize,
}

#[derive(PartialEq, Debug)]
enum EvalResult {
    True(bool),
    False(bool),
    Rewind(usize),
    ResetNode,
}

trait Node {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult;
    fn reset(&mut self);
}

#[derive(Debug)]
enum NodeState {
    True,
    False,
}

struct Limits {}

impl Limits {
    fn check(&mut self) -> EvalResult {
        unimplemented!("Limits::check");
    }
    fn reset(&mut self) {
        unimplemented!("Limits::check");
    }
}

struct And<'a> {
    state: Option<NodeState>,
    children: Option<Vec<&'a mut dyn Node>>,
    limits: Option<Limits>,
}

impl<'a> Node for And<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        // node is stateful by default
        let mut is_stateful: bool = true;
        if let Some(children) = &mut self.children {
            for c in children.iter_mut() {
                match c.evaluate(ctx) {
                    EvalResult::True(stateful) => {
                        // node is stateful only if all evaluations are true and stateful
                        if !stateful {
                            is_stateful = false
                        }
                    }
                    EvalResult::False(stateful) => {
                        if stateful {
                            self.state = Some(NodeState::False)
                        }
                        return EvalResult::False(stateful);
                    }
                    _ => panic!(),
                }
            }
        }
        if is_stateful {
            self.state = Some(NodeState::True);
            return EvalResult::True(true);
        }
        EvalResult::True(false)
    }

    fn reset(&mut self) {
        self.state = None;
        if let Some(children) = &mut self.children {
            for c in children.iter_mut() {
                c.reset()
            }
        }
        if let Some(limits) = &mut self.limits {
            limits.reset();
        }
    }
}

struct Or<'a> {
    state: Option<NodeState>,
    children: Option<Vec<&'a mut dyn Node>>,
}

impl<'a> Node for Or<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        let mut is_stateful: bool = true;
        if let Some(children) = &mut self.children {
            for c in children.iter_mut() {
                match c.evaluate(ctx) {
                    EvalResult::True(stateful) => {
                        if stateful {
                            self.state = Some(NodeState::True);
                        }
                        return EvalResult::True(stateful);
                    }
                    EvalResult::False(stateful) => {
                        if !stateful {
                            is_stateful = false;
                        }
                    }
                    _ => panic!(),
                }
            }
        }
        EvalResult::False(is_stateful)
    }

    fn reset(&mut self) {
        self.state = None;
        if let Some(children) = &mut self.children {
            for c in children.iter_mut() {
                c.reset()
            }
        }
    }
}

enum ThenBranch {
    Left,
    Right,
}

struct Then<'a> {
    left: &'a mut dyn Node,
    right: &'a mut dyn Node,
    state: Option<NodeState>,
    branch: ThenBranch,
}

impl<'a> Node for Then<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }

        if let ThenBranch::Left = self.branch {
            return match self.left.evaluate(ctx) {
                EvalResult::True(_) => {
                    self.branch = ThenBranch::Right;
                    EvalResult::False(false)
                }
                EvalResult::False(true) => {
                    EvalResult::False(true)
                }
                _ => {
                    EvalResult::False(false)
                }
            };
        }

        if let ThenBranch::Right = self.branch {
            let result = self.right.evaluate(ctx);
            if let EvalResult::True(true) = result {
                self.state = Some(NodeState::True);
            }

            return result;
        }

        return EvalResult::False(false);
    }

    fn reset(&mut self) {
        self.state = None;
        self.branch = ThenBranch::Left;
        self.left.reset();
        self.right.reset();
    }
}

struct ScalarValue<T, C> {
    c: PhantomData<C>,
    state: Option<NodeState>,
    is_partition: bool,
    left: T,
    right: T,
}

impl<T, C> Node for ScalarValue<T, C> where T: Copy, C: cmp::Cmp<T> {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        if C::is_true(self.left, self.right) {
            if self.is_partition {
                self.state = Some(NodeState::True);
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_partition {
            self.state = Some(NodeState::False);
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = None;
    }
}

struct VectorValue<T, C> {
    c: PhantomData<C>,
    state: Option<NodeState>,
    is_partition: bool,
    left: Vec<T>,
    right: T,
}

impl<T, C> Node for VectorValue<T, C> where T: Copy, C: cmp::Cmp<T> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        if C::is_true(self.left[ctx.row_id], self.right) {
            if self.is_partition {
                self.state = Some(NodeState::True);
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_partition {
            self.state = Some(NodeState::False);
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = None;
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn scalar_value_equal_fails() {
        let mut n = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 1,
        };


        let ctx = Context::default();

        assert_eq!(n.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn scalar_value_equal() {
        let mut n = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };


        let ctx = Context::default();

        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn scalar_value_equal_fails_stateful() {
        let mut n = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 2,
            right: 1,
        };

        let ctx = Context::default();

        assert_eq!(n.evaluate(&ctx), EvalResult::False(true))
    }


    #[test]
    fn scalar_value_stateful() {
        let mut n = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 1,
        };


        let ctx = Context::default();
        assert_eq!(n.evaluate(&ctx), EvalResult::True(true))
    }

    #[test]
    fn vector_value_equal_fails() {
        let mut n = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 0 };
        assert_eq!(n.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn vector_value_equal() {
        let mut n = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 1 };
        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn vector_value_equal_fails_stateful() {
        let mut n = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 0 };
        assert_eq!(n.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn vector_value_equal_stateful() {
        let mut n = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: vec![0, 1],
            right: 1,
        };

        let ctx = Context { row_id: 1 };
        assert_eq!(n.evaluate(&ctx), EvalResult::True(true))
    }

    #[test]
    fn a_and_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };
        let mut q = And {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn a_and_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };
        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_stateful_and_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_stateful_and_b_stateful() {
        let mut a = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: vec![1, 2],
            right: 1,
        };

        let mut b = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: vec![2, 3],
            right: 2,
        };

        let mut q = And {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
            limits: None,
        };

        let mut ctx = Context { row_id: 0 };
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
        ctx.row_id = 1;
        // check for stateful
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn a_stateful_and_b_stateful_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn a_or_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 1,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn a_or_stateful_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 1,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_stateful_b_stateful() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: vec![1, 2],
            right: 1,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let mut ctx = Context { row_id: 0 };
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
        ctx.row_id = 1;
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn a_or_stateful_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn stateful_a_or_stateful_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut q = Or {
            state: None,
            children: Some(vec![
                &mut a,
                &mut b,
            ]),
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn then() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Then {
            left: &mut a,
            right: &mut b,
            state: None,
            branch: ThenBranch::Left,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
    }

    #[test]
    fn then_right_fail() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 2,
        };

        let mut q = Then {
            left: &mut a,
            right: &mut b,
            state: None,
            branch: ThenBranch::Left,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn then_left_stateful_fail() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Then {
            left: &mut a,
            right: &mut b,
            state: None,
            branch: ThenBranch::Left,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
    }
}

fn main() {}
