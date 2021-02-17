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
            let result = self.left.evaluate(ctx);
            match result {
                EvalResult::True(_) => self.branch = ThenBranch::Right,
                _ => return result,
            }
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

impl<T, C> Node for ScalarValue<T, C>
where
    T: Copy,
    C: cmp::Cmp<T>,
{
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

impl<T, C> Node for VectorValue<T, C>
where
    T: Copy,
    C: cmp::Cmp<T>,
{
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
        let mut v: ScalarValue<u32, cmp::Equal> = ScalarValue {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 1,
        };

        let n: &mut dyn Node = &mut v;

        let ctx = Context::default();

        assert_eq!(n.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn scalar_value_equal() {
        let mut v: ScalarValue<u32, cmp::Equal> = ScalarValue {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let n: &mut dyn Node = &mut v;

        let ctx = Context::default();

        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn vector_value_equal_fails() {
        let mut v: VectorValue<u32, cmp::Equal> = VectorValue {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: vec![0, 1],
            right: 1,
        };

        let n: &mut dyn Node = &mut v;

        let ctx = Context { row_id: 0 };

        assert_eq!(n.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn vector_value_equal() {
        let mut v: VectorValue<u32, cmp::Equal> = VectorValue {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: vec![0, 1],
            right: 1,
        };

        let n: &mut dyn Node = &mut v;

        let ctx = Context { row_id: 1 };

        assert_eq!(n.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_and_b() {
        let scalar1 = &mut ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 1,
            right: 1,
        };
        let scalar2 = &mut ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: None,
            is_partition: false,
            left: 2,
            right: 2,
        };
        let q: &mut dyn Node = &mut And {
            state: None,
            children: Some(vec![scalar1, scalar2]),
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }
}

fn main() {
    /*let mut q = Node {
        children: Some(vec![Node {
            children: None,
            count: 0,
        }]),
        count: 0,
    };*/
    let or = &mut Or {
        state: None,
        children: None,
    };

    let q: &mut dyn Node = &mut And {
        state: None,
        children: Some(vec![or]),
        limits: None,
    };
    let ctx = Context::default();

    dbg!(q.evaluate(&ctx));
}
