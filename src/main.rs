struct Context {
    row_id: usize,
}

pub trait Node {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult;
    fn reset(&mut self);
}

enum EvalResult {
    True(bool),
    False(bool),
    Rewind(usize),
    ResetNode,
}

enum NodeState {
    True,
    False,
}

struct Limits {}

/*impl Limits {
    pub fn check(&mut self) -> EvalResult {}
    pub fn reset(&mut self) {}
}*/

struct And {
    state: Option<NodeState>,
    children: Vec<dyn Node>,
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
        for c in self.children.iter_mut() {
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
            }
        }

        if is_stateful {
            self.state = Some(NodeState::True);
            EvalResult::True(true)
        }

        EvalResult::True(false)
    }

    fn reset(&mut self) {
        self.state = None;
        for c in self.children.iter_mut() {
            c.reset()
        }

        if let Some(limits) = &mut self.limits {
            limits.reset();
        }
    }
}

struct Or {
    state: Option<NodeState>,
    children: Vec<dyn Node>,
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
        for c in self.children.iter_mut() {
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
            }
        }

        EvalResult::False(is_stateful)
    }

    fn reset(&mut self) {
        self.state = None;
        for c in self.children.iter_mut() {
            c.reset()
        }
    }
}

enum ThenBranch {
    Left,
    Right,
}

struct Then {
    left: dyn Node,
    right: dyn Node,
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
            match result = self.left.evaluate(ctx) {
                EvalResult::True(_) => self.branch = ThenBranch::Right,
                _ => return result
            }
        }

        if let ThenBranch::Right = self.branch {
            match result = self.right.evaluate(ctx) {
                EvalResult::True(stateful) => {
                    if stateful {
                        self.state = Some(NodeState::True);
                    }

                    return result;
                }
                _ => return result
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

trait Cmp<T> {
    fn is_true(left: T, right: T) -> bool;
    fn is_false(left: T, right: T) -> bool;
}

struct Equal;

impl Cmp<u32> for Equal {
    fn is_true(left: u32, right: u32) -> bool {
        left == right
    }

    fn is_false(left: u32, right: u32) -> bool {
        left != right
    }
}

struct ScalarValue<T> {
    state: Option<NodeState>,
    is_partition: bool,
    cmp: dyn Cmp<T>,
    needle: T,
    value: T,
}

impl Node for ScalarValue<T> {
    fn evaluate<T, C: Cmp<T>>(&mut self, _: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }

        match C::is_true(self.value, self.needle) {
            true => {
                if self.is_partition {
                    self.state = Some(NodeState::True);
                    EvalResult::True(true)
                }
                EvalResult::True(false)
            }
            false => {
                if self.is_partition {
                    self.state = Some(NodeState::False);
                    EvalResult::False(true)
                }
                EvalResult::False(false)
            }
        }
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

impl Node for VectorValue<T> {
    fn evaluate<T, C: Cmp<T>>(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        if let Some(state) = &self.state {
            return match state {
                NodeState::True => EvalResult::True(true),
                NodeState::False => EvalResult::False(true),
            };
        }

        match C::is_true(self.values[&ctx.row_id], self.needle) {
            true => {
                if self.is_partition {
                    self.state = Some(NodeState::True);
                    EvalResult::True(true)
                }
                EvalResult::True(false)
            }
            false => {
                if self.is_partition {
                    self.state = Some(NodeState::False);
                    EvalResult::False(true)
                }
                EvalResult::False(false)
            }
        }
    }

    fn reset(&mut self) {
        self.state = None;
    }
}

fn main() {
    let mut q = Node {
        children: Some(vec![
            Node {
                children: None,
                count: 0,
            }]),
        count: 0,
    };

    q.eval();
    println!("{}", q.children.unwrap()[0].count);
}
