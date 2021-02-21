use std::marker::PhantomData;
use std::time::{Instant};
use chrono::{DateTime, Utc, NaiveDateTime};
use std::convert::{From, TryInto, TryFrom};

mod cmp;

#[derive(Default)]
struct Context {
    row_id: usize,
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum EvalResult {
    True(bool),
    False(bool),
}

trait Node {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult;
    fn reset(&mut self);
}

#[derive(PartialEq, Debug)]
enum NodeState {
    None,
    True,
    False,
}

trait Limit {
    fn check(&mut self, ctx: &Context, matched: bool) -> LimitCheckResult;
    fn reset(&mut self);
}

#[derive(PartialEq, Debug)]
enum LimitCheckResult {
    True,
    False,
    ResetNode,
}

struct CountLimit {
    min: u32,
    max: u32,
    current_value: u32,
}

/*impl Limit for CountLimit {
    fn check(&mut self, matched: bool) -> Option<LimitCheckResult> {
        if matched {
            self.current_value += 1;

            if self.current_value < self.min {
                return Some(LimitCheckResult::False);
            } else if self.max > 0 && self.current_value > self.max {
                return Some(LimitCheckResult::ResetNode);
            }

            return None;
        }

        if self.current_value > self.min &&
            (self.max == 0 || self.current_value <= self.max) {
            // node became stateful
            return Some(LimitCheckResult::True);
        }

        return None;
    }

    fn reset(&mut self) {
        self.current_value = 0;
    }
}

struct RowLimit {
    min: u32,
    max: u32,
    current_value: u32,
}

impl Limit for RowLimit {
    fn check(&mut self, matched: bool) -> Option<LimitCheckResult> {
        if matched {
            self.current_value += 1;

            if self.current_value < self.min {
                return Some(LimitCheckResult::False);
            } else if self.max > 0 && self.current_value > self.max {
                return Some(LimitCheckResult::ResetNode);
            }

            return None;
        }

        if self.current_value > self.min &&
            (self.max == 0 || self.current_value <= self.max) {
            // node became stateful
            return Some(LimitCheckResult::True);
        }

        return None;
    }

    fn reset(&mut self) {
        self.current_value = 0;
    }
}
*/
struct AbsoluteTimeWindowLimit<'a, T> {
    from: Option<T>,
    to: Option<T>,
    values: &'a [T],
}

fn example(v: i8) -> i32 {
    i32::try_from(v).unwrap()
}

impl<'a, T: TryFrom<i64>> AbsoluteTimeWindowLimit<'a, T> {
    pub fn new<Tz: chrono::TimeZone>(values: &'a [T], from: Option<chrono::DateTime<Tz>>, to: Option<chrono::DateTime<Tz>>) -> Self {
        let mut obj = AbsoluteTimeWindowLimit {
            from: None,
            to: None,
            values,
        };

        if let Some(t) = from {
            obj.from = t.timestamp().try_into().ok();
        }

        if let Some(t) = to {
            obj.to = t.timestamp().try_into().ok();
        }

        obj
    }
}

impl<'a, T> Limit for AbsoluteTimeWindowLimit<'a, T> where T: PartialOrd + Copy {
    fn check(&mut self, ctx: &Context, matched: bool) -> LimitCheckResult {
        let ts = self.values[ctx.row_id];

        if let Some(from) = self.from {
            if ts < from {
                return LimitCheckResult::False;
            }
        }

        if let Some(to) = self.to {
            if ts <= to {
                return LimitCheckResult::True;
            }

            return LimitCheckResult::ResetNode;
        }

        panic!("unreachable code");
    }

    fn reset(&mut self) {}
}


struct Limits<'a> {
    limits: Vec<&'a mut dyn Limit>,
}

impl<'a> Limits<'a> {
    pub fn check(&mut self, eval_result: EvalResult) -> Option<LimitCheckResult> {
        panic!("unimplemented");
        /*let mut matched: bool = false;
        match eval_result {
            EvalResult::True(true) | EvalResult::False(true) => return None,
            EvalResult::True(false) => matched = true,
            _ => {}
        }

        let mut is_false: bool = false;
        for l in self.limits.iter_mut() {
            match l.check(matched) {
                Some(LimitCheckResult::ResetNode) => return Some(LimitCheckResult::ResetNode),
                Some(LimitCheckResult::True) => return Some(LimitCheckResult::True),
                Some(LimitCheckResult::False) => is_false = true,
                _ => {}
            }
        }

        if is_false {
            return Some(LimitCheckResult::False);
        }

        None*/
    }
    fn reset(&mut self) {}
}

struct And<'a> {
    state: NodeState,
    children: Vec<&'a mut dyn Node>,
    limits: Option<&'a mut Limits<'a>>,
}

impl<'a> Node for And<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

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
                // if some failed nodes is stateful, then it make current node failed stateful  as well
                EvalResult::False(stateful) => {
                    if stateful {
                        self.state = NodeState::False;
                    }
                    return self.check_limits(EvalResult::False(stateful));
                }
            }
        }

        if is_stateful {
            self.state = NodeState::True;
            return self.check_limits(EvalResult::True(true));
        }

        self.check_limits(EvalResult::True(false))
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        for c in self.children.iter_mut() {
            c.reset()
        }
        if let Some(limits) = &mut self.limits {
            limits.reset();
        }
    }
}

impl<'a> And<'a> {
    fn check_limits(&mut self, eval_result: EvalResult) -> EvalResult {
        if let Some(limits) = &mut self.limits {
            /*            return match limits.check(eval_result) {
                            None => eval_result,
                            Some(LimitCheckResult::True) => EvalResult::True(false),
                            Some(LimitCheckResult::False) => EvalResult::False(false),
                            Some(LimitCheckResult::ResetNode) => {
                                self.reset();
                                EvalResult::False(false)
                            }
                        };*/
        }

        eval_result
    }
}

struct Or<'a> {
    state: NodeState,
    children: Vec<&'a mut dyn Node>,
}

impl<'a> Node for Or<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let mut is_stateful: bool = true;
        for c in self.children.iter_mut() {
            match c.evaluate(ctx) {
                EvalResult::True(stateful) => {
                    if stateful {
                        self.state = NodeState::True;
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
        EvalResult::False(is_stateful)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        for c in self.children.iter_mut() {
            c.reset()
        }
    }
}

enum ThenBranch {
    Left,
    Right,
}
/*
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
}*/

struct SequenceNode<'a> {
    node: &'a mut dyn Node,
    limits: Option<&'a mut Limits<'a>>,
}

impl<'a> SequenceNode<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        self.node.evaluate(ctx)
    }
    fn check_limits(&mut self, eval_result: EvalResult) -> Option<LimitCheckResult> {
        Some(LimitCheckResult::True)
    }
}

struct Sequence<'a> {
    current_node_idx: usize,
    nodes: Vec<SequenceNode<'a>>,
    state: NodeState,
}

impl<'a> Node for Sequence<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        for (idx, node) in self.nodes.iter_mut().enumerate() {
            if idx == self.current_node_idx {
                return match node.evaluate(ctx) {
                    EvalResult::False(stateful) => {
                        if stateful {
                            self.state = NodeState::False;
                        }

                        EvalResult::False(stateful)
                    }
                    EvalResult::True(_) => {
                        // last node
                        if idx == self.nodes.len() - 1 {
                            self.state = NodeState::True;
                            return EvalResult::True(true);
                        }

                        self.current_node_idx += 1;
                        EvalResult::False(false)
                    }
                };
            }
        }

        panic!("unreachable code");
    }

    fn reset(&mut self) {
        unimplemented!()
    }
}

struct ScalarValue<T, C> {
    c: PhantomData<C>,
    state: NodeState,
    is_partition: bool,
    left: T,
    right: T,
}

impl<T, C> Node for ScalarValue<T, C> where T: Copy, C: cmp::Cmp<T> {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        if C::is_true(self.left, self.right) {
            if self.is_partition {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_partition {
            self.state = NodeState::False;
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
    }
}

struct VectorValue<T, C> {
    c: PhantomData<C>,
    state: NodeState,
    is_partition: bool,
    left: Vec<T>,
    right: T,
}

impl<T, C> Node for VectorValue<T, C> where T: Copy, C: cmp::Cmp<T> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };
        if C::is_true(self.left[ctx.row_id], self.right) {
            if self.is_partition {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            return EvalResult::True(false);
        }
        if self.is_partition {
            self.state = NodeState::False;
            return EvalResult::False(true);
        }
        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
    }
}

struct TestValue {
    is_partition: bool,
    value: bool,
}

impl Node for TestValue {
    fn evaluate(&mut self, _: &Context) -> EvalResult {
        if self.value {
            return EvalResult::True(self.is_partition);
        }
        return EvalResult::False(self.is_partition);
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
    use crate::*;
    use chrono::DateTime;

    #[test]
    fn scalar_value_equal_fails() {
        let mut n = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
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
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };
        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn a_and_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };
        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_stateful_and_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_stateful_and_b_stateful() {
        let mut a = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![1, 2],
            right: 1,
        };

        let mut b = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![2, 3],
            right: 2,
        };

        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
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
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 2,
            right: 2,
        };

        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn a_or_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 1,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn a_or_stateful_b() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 1,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false))
    }

    #[test]
    fn a_or_stateful_b_stateful() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = VectorValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: vec![1, 2],
            right: 1,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
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
            state: NodeState::None,
            is_partition: false,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false))
    }

    #[test]
    fn stateful_a_or_stateful_b_fails() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 2, //fails
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true))
    }

    #[test]
    fn sequence() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Sequence {
            current_node_idx: 0,
            nodes: vec![
                SequenceNode { node: &mut a, limits: None },
                SequenceNode { node: &mut b, limits: None },
            ],
            state: NodeState::None,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    /*#[test]
    fn then_right_fail() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 2,
        };

        let mut q = Then {
            left: &mut a,
            right: &mut b,
            state: NodeState::None,
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
            state: NodeState::None,
            is_partition: true,
            left: 1,
            right: 2,
        };

        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 2,
        };

        let mut q = Then {
            left: &mut a,
            right: &mut b,
            state: NodeState::None,
            branch: ThenBranch::Left,
        };

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
    }*/

    #[test]
    fn sequence_scenario() {
        let mut a = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };
        let mut b_fail = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 2,
            right: 1,
        };
        let mut b = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };
        let mut c = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut s = Sequence {
            current_node_idx: 0,
            nodes: vec![
                SequenceNode { node: &mut a, limits: None },
                SequenceNode { node: &mut b_fail, limits: None },
                SequenceNode { node: &mut c, limits: None },
            ],
            state: NodeState::None,
        };

        let ctx = Context::default();
        // pass first node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.current_node_idx, 1);
        assert_eq!(s.state, NodeState::None);
        // fail at second node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.current_node_idx, 1);
        assert_eq!(s.state, NodeState::None);
        // set second node to True
        s.nodes[1].node = &mut b;
        // pass second node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.current_node_idx, 2);
        assert_eq!(s.state, NodeState::None);
        // pass third node
        assert_eq!(s.evaluate(&ctx), EvalResult::True(true));
        assert_eq!(s.current_node_idx, 2);
        assert_eq!(s.state, NodeState::True);
        // nothing changes on further evaluations
        assert_eq!(s.evaluate(&ctx), EvalResult::True(true));
        assert_eq!(s.current_node_idx, 2);
        assert_eq!(s.state, NodeState::True);
    }

    /*#[test]
    fn sequence_limits() {
        let mut a = TestValue {
            is_partition: false,
            value: true,
        };
        let mut b = TestValue {
            is_partition: false,
            value: false,
        };
        let mut c = TestValue {
            is_partition: false,
            value: false,
        };

        let mut a_count_limit = CountLimit {
            min: 0,
            max: 2,
            current_value: 0,
        };
        let mut a_row_limit = RowLimit {
            min: 1,
            max: 3,
            current_value: 0,
        };

        let mut a_limits = Limits {
            limits: vec![&mut a_count_limit, &mut a_row_limit],
        };


        let mut b_count_limit = CountLimit {
            min: 0,
            max: 2,
            current_value: 0,
        };

        let mut b_row_limit = RowLimit {
            min: 1,
            max: 3,
            current_value: 0,
        };

        let mut b_limits = Limits {
            limits: vec![&mut b_count_limit, &mut b_row_limit],
        };

        let mut s = Sequence {
            current_node_idx: 0,
            nodes: vec![
                SequenceNode { node: &mut a, limits: Some(&mut a_limits) },
                SequenceNode { node: &mut b, limits: Some(&mut b_limits) },
                SequenceNode { node: &mut c, limits: None },
            ],
            state: NodeState::None,
        };

        let ctx = Context::default();
        // pass first node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.current_node_idx, 0);
        assert_eq!(s.state, NodeState::None);
    }*/

    #[test]
    fn absolute_time_window_limit() {
        let from = Some(DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(1, 0), Utc));
        let to = Some(DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(2, 0), Utc));

        let vals: Vec<u32> = vec![0, 1, 2, 3, 4];
        let mut limit = AbsoluteTimeWindowLimit::new(&vals, from, to);
        let mut ctx = Context { row_id: 0 };
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::False);
        ctx.row_id = 1;
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::True);
        ctx.row_id = 2;
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::True);
        ctx.row_id = 3;
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::ResetNode);
    }
}

fn main() {}
