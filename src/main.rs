#[derive(Default)]
struct Context {
    row_id: usize,
}

#[derive(Debug)]
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

struct And {
    state: Option<NodeState>,
    children: Option<Vec<Box<dyn Node>>>,
    limits: Option<Limits>,
}

impl Node for And {
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

struct Or {
    state: Option<NodeState>,
    children: Option<Vec<Box<dyn Node>>>,
}

impl Node for Or {
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

struct Then {
    left: Box<dyn Node>,
    right: Box<dyn Node>,
    state: Option<NodeState>,
    branch: ThenBranch,
}

impl Node for Then {
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
            match result {
                EvalResult::True(stateful) => {
                    if stateful {
                        self.state = Some(NodeState::True);
                    }

                    return result;
                }
                _ => return result,
            }
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

struct ScalarValue<T: PartialEq> {
    state: Option<NodeState>,
    is_partition: bool,
    needle: T,
    value: T,
}

impl<T: PartialEq> Node for ScalarValue<T> {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        if self.value == self.needle {
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

struct VectorValue<T> {
    state: Option<NodeState>,
    is_partition: bool,
    needle: T,
    values: Vec<T>,
}

impl<T: PartialEq> Node for VectorValue<T> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }
        if self.values[ctx.row_id] == self.needle {
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

fn main() {
    /*let mut q = Node {
        children: Some(vec![Node {
            children: None,
            count: 0,
        }]),
        count: 0,
    };*/

    let mut q: Box<dyn Node> = Box::new(And {
        state: None,
        children: Some(vec![Box::new(Or {
            state: None,
            children: None,
        })]),
        limits: None,
    });
    let ctx = Context::default();

    dbg!(q.evaluate(&ctx));
}
