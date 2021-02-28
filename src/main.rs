use std::marker::PhantomData;
use std::time::{Instant};
use chrono::{DateTime, Utc, NaiveDateTime};
use std::convert::{From, TryInto, TryFrom};
use crate::cmp::Cmp;
use std::fmt;

mod cmp;

#[derive(Default)]
struct Context {
    row_id: usize,
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum EvalResult {
    True(bool),
    False(bool),
    ResetNode,
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

#[derive(Debug, Copy, Clone)]
enum CmpValue<T> {
    None,
    Equal(T),
    NotEqual(T),
    Less(T),
    LessEqual(T),
    Greater(T),
    GreaterEqual(T),
}

impl<T> CmpValue<T> where T: Copy + PartialEq + PartialOrd {
    pub fn value(self) -> Option<T> {
        return match self {
            CmpValue::None => None,
            CmpValue::Equal(v) | CmpValue::NotEqual(v) |
            CmpValue::Less(v) | CmpValue::LessEqual(v) |
            CmpValue::Greater(v) |
            CmpValue::GreaterEqual(v) => Some(v),
        };
    }
    pub fn is_set(self) -> bool {
        return match self {
            CmpValue::None => false,
            _ => true,
        };
    }
    pub fn cmp(self, left: T) -> bool {
        return match self {
            CmpValue::None => true,
            CmpValue::Equal(right) => cmp::Equal::is_true(left, right),
            CmpValue::NotEqual(right) => cmp::NotEqual::is_true(left, right),
            CmpValue::Less(right) => cmp::Less::is_true(left, right),
            CmpValue::LessEqual(right) => cmp::LessEqual::is_true(left, right),
            CmpValue::Greater(right) => cmp::GreaterEqual::is_true(left, right),
            CmpValue::GreaterEqual(right) => cmp::GreaterEqual::is_true(left, right),
        };
    }
}

impl<T> CmpValue<T> {
    // https://github.com/rust-lang/rust/issues/50133
    pub fn to<C: TryFrom<T>>(self) -> CmpValue<C> {
        return match self {
            CmpValue::None => CmpValue::None,
            CmpValue::Equal(t) => CmpValue::Equal(t.try_into().ok().unwrap()),
            CmpValue::NotEqual(t) => CmpValue::NotEqual(t.try_into().ok().unwrap()),
            CmpValue::Less(t) => CmpValue::Less(t.try_into().ok().unwrap()),
            CmpValue::LessEqual(t) => CmpValue::LessEqual(t.try_into().ok().unwrap()),
            CmpValue::Greater(t) => CmpValue::Greater(t.try_into().ok().unwrap()),
            CmpValue::GreaterEqual(t) => CmpValue::GreaterEqual(t.try_into().ok().unwrap()),
        };
    }
}

impl<'a, T: TryFrom<i64>> AbsoluteTimeWindowLimit<'a, T> {
    pub fn new(values: &'a [T], node: &'a mut dyn Node, from: CmpValue<i64>, to: CmpValue<i64>) -> Self {
        AbsoluteTimeWindowLimit {
            from: from.to(),
            to: to.to(),
            values,
            node,
            state: NodeState::None,
            is_grouped: false,
        }
    }

    pub fn new_grouped(values: &'a [T], node: &'a mut dyn Node, from: CmpValue<i64>, to: CmpValue<i64>) -> Self {
        AbsoluteTimeWindowLimit {
            from: from.to(),
            to: to.to(),
            values,
            node,
            state: NodeState::None,
            is_grouped: true,
        }
    }
}

struct AbsoluteTimeWindowLimit<'a, T> {
    from: CmpValue<T>,
    to: CmpValue<T>,
    values: &'a [T],
    node: &'a mut dyn Node,
    state: NodeState,
    is_grouped: bool, // grouped limit just return ResetNode no parent, ungrouped limit resets all children
}

impl<'a, T> Node for AbsoluteTimeWindowLimit<'a, T> where T: Copy + PartialEq + PartialOrd {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        let ts = self.values[ctx.row_id];

        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        // evaluate main node
        let result = self.node.evaluate(ctx);

        return match result {
            EvalResult::True(true) => {
                self.state = NodeState::True;
                EvalResult::True(true)
            }
            EvalResult::False(true) => {
                self.state = NodeState::False;
                EvalResult::False(true)
            }
            EvalResult::ResetNode => {
                if self.is_grouped {
                    // just return resetNode state no children
                    return EvalResult::ResetNode;
                }
                // reset itself and children
                self.reset();
                return EvalResult::False(false);
            }
            _ => {
                let from = self.from.cmp(ts);
                let to = self.to.cmp(ts);
                if from && to {
                    return result;
                }
                if self.to.is_set() && !to {
                    if self.is_grouped {
                        return EvalResult::ResetNode;
                    }
                    self.reset();
                    return EvalResult::False(false);
                }

                EvalResult::False(false)
            }
        };
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        self.node.reset();
    }
}

struct TrueCountLimit<'a> {
    state: NodeState,
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
    node: &'a mut dyn Node,
    is_grouped: bool,
}

impl<'a> TrueCountLimit<'a> {
    pub fn new(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        TrueCountLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: false,
        }
    }

    pub fn new_grouped(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        TrueCountLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: true,
        }
    }
}

impl<'a> Node for TrueCountLimit<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let result = self.node.evaluate(ctx);
        match result {
            EvalResult::True(true) => {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            EvalResult::True(false) => {
                self.count += 1;
            }
            EvalResult::False(true) => {
                self.state = NodeState::False;
                return EvalResult::False(true);
            }
            EvalResult::ResetNode => {
                if self.is_grouped {
                    return EvalResult::ResetNode;
                }
                self.reset();
                return EvalResult::False(false);
            }
            _ => {}
        };

        let from = self.from.cmp(self.count);
        let to = self.to.cmp(self.count);
        if from && to {
            return result;
        }


        if self.to.is_set() && !to {
            if self.is_grouped {
                return EvalResult::ResetNode;
            }
            self.reset();
            return EvalResult::False(false);
        }

        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.count = 0;
        self.state = NodeState::None;
        self.node.reset();
    }
}


struct RowLimit<'a> {
    state: NodeState,
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
    node: &'a mut dyn Node,
    is_grouped: bool,
}

impl<'a> RowLimit<'a> {
    pub fn new(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        RowLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: false,
        }
    }

    pub fn new_grouped(node: &'a mut dyn Node, from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        RowLimit {
            state: NodeState::None,
            node,
            from,
            to,
            count: 0,
            is_grouped: true,
        }
    }
}

impl<'a> Node for RowLimit<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        // check if node already has state
        match self.state {
            NodeState::True => return EvalResult::True(true),
            NodeState::False => return EvalResult::False(true),
            _ => {}
        };

        let result = self.node.evaluate(ctx);
        self.count += 1;
        match result {
            EvalResult::True(true) => {
                self.state = NodeState::True;
                return EvalResult::True(true);
            }
            EvalResult::True(false) => {}
            EvalResult::False(true) => {
                self.state = NodeState::False;
                return EvalResult::False(true);
            }
            EvalResult::ResetNode => {
                if self.is_grouped {
                    return EvalResult::ResetNode;
                }
                self.reset();
                return EvalResult::False(false);
            }
            _ => {}
        };

        let from = self.from.cmp(self.count);
        let to = self.to.cmp(self.count);
        if from && to {
            return result;
        }


        if self.to.is_set() && !to {
            if self.is_grouped {
                return EvalResult::ResetNode;
            }
            self.reset();
            return EvalResult::False(false);
        }

        EvalResult::False(false)
    }

    fn reset(&mut self) {
        self.count = 0;
        self.state = NodeState::None;
        self.node.reset();
    }
}

struct And<'a> {
    state: NodeState,
    children: Vec<&'a mut dyn Node>,
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
                EvalResult::False(stateful) => {
                    // if some failed nodes is stateful, then it make current node failed stateful  as well
                    if stateful {
                        self.state = NodeState::False;
                    }
                    return EvalResult::False(stateful);
                }
                EvalResult::ResetNode => {
                    self.reset();
                    return EvalResult::False(false);
                }
            }
        }

        if is_stateful {
            self.state = NodeState::True;
            return EvalResult::True(true);
        }

        EvalResult::True(false)
    }

    fn reset(&mut self) {
        self.state = NodeState::None;
        for c in self.children.iter_mut() {
            c.reset()
        }
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
                EvalResult::ResetNode => {
                    self.reset();
                    return EvalResult::False(false);
                }
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


#[derive(PartialEq, Debug, Copy, Clone)]
enum SequenceState {
    True,
    False,
    LeftNode,
    RightNode,
}

struct Sequence<'a> {
    left_node: &'a mut dyn Node,
    right_node: &'a mut dyn Node,
    state: SequenceState,
}

impl<'a> Sequence<'a> {
    fn new(left: &'a mut dyn Node, right: &'a mut dyn Node) -> Self {
        Sequence {
            left_node: left,
            right_node: right,
            state: SequenceState::LeftNode,
        }
    }
}

impl<'a> Node for Sequence<'a> {
    fn evaluate(&mut self, ctx: &Context) -> EvalResult {
        match self.state {
            SequenceState::True => {
                return EvalResult::True(true);
            }
            SequenceState::False => {
                return EvalResult::False(true);
            }
            SequenceState::LeftNode => {
                return match self.left_node.evaluate(ctx) {
                    EvalResult::True(_) => {
                        self.state = SequenceState::RightNode;
                        EvalResult::False(false)
                    }
                    EvalResult::False(true) => {
                        self.state = SequenceState::False;
                        EvalResult::False(true)
                    }
                    EvalResult::False(false) => EvalResult::False(false),
                    EvalResult::ResetNode => {
                        self.reset();
                        EvalResult::False(false)
                    }
                };
            }

            SequenceState::RightNode => {
                return match self.right_node.evaluate(ctx) {
                    EvalResult::True(_) => {
                        self.state = SequenceState::True;
                        EvalResult::True(true)
                    }
                    EvalResult::False(true) => {
                        self.state = SequenceState::False;
                        EvalResult::False(true)
                    }
                    EvalResult::False(false) => EvalResult::False(false),
                    EvalResult::ResetNode => {
                        self.reset();
                        EvalResult::False(false)
                    }
                };
            }
        }
    }

    fn reset(&mut self) {
        self.state = SequenceState::LeftNode;
        self.left_node.reset();
        self.right_node.reset();
    }
}

struct ScalarValue<T, C> {
    c: PhantomData<C>,
    state: NodeState,
    is_partition: bool,
    left: T,
    right: T,
}

impl<T, C> ScalarValue<T, C> {
    pub fn new(left: T, right: T) -> Self {
        ScalarValue {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left,
            right,
        }
    }
}

impl<T, C> Node for ScalarValue<T, C> where T: Copy, C: Cmp<T> {
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
    use chrono::{DateTime, TimeZone};

    #[test]
    fn cmp() {
        assert_eq!(<cmp::Equal as cmp::Cmp<u32>>::is_true(1, 2), false);
        assert_eq!(<cmp::Equal as cmp::Cmp<u32>>::is_true(1, 1), true);

        assert_eq!(<cmp::Less as cmp::Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<cmp::Less as cmp::Cmp<u32>>::is_true(1, 1), false);

        assert_eq!(<cmp::LessEqual as cmp::Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<cmp::LessEqual as cmp::Cmp<u32>>::is_true(2, 2), true);
        assert_eq!(<cmp::LessEqual as cmp::Cmp<u32>>::is_true(3, 2), false);

        assert_eq!(<cmp::Greater as cmp::Cmp<u32>>::is_true(2, 1), true);
        assert_eq!(<cmp::Greater as cmp::Cmp<u32>>::is_true(1, 1), false);

        assert_eq!(<cmp::GreaterEqual as cmp::Cmp<u32>>::is_true(2, 1), true);
        assert_eq!(<cmp::GreaterEqual as cmp::Cmp<u32>>::is_true(2, 2), true);
        assert_eq!(<cmp::GreaterEqual as cmp::Cmp<u32>>::is_true(2, 3), false);

        assert_eq!(<cmp::NotEqual as cmp::Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<cmp::NotEqual as cmp::Cmp<u32>>::is_true(1, 1), false);
    }

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

        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(true));
    }

    #[test]
    fn sequence_right_fail() {
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

        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn sequence_left_stateful_fail() {
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

        let mut q = Sequence::new(&mut a, &mut b);

        let ctx = Context::default();
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(true));
    }

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

        let mut s = Sequence::new(&mut a, &mut b_fail);

        let ctx = Context::default();
        // pass first node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        // fail at second node
        assert_eq!(s.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(s.state, SequenceState::RightNode);
        // set second node to True
        s.right_node = &mut b;
        // pass second node
        assert_eq!(s.evaluate(&ctx), EvalResult::True(true));
        assert_eq!(s.state, SequenceState::True);
    }

    #[test]
    fn absolute_time_window_limit() {
        let mut node = TestValue { is_partition: false, value: true };
        let mut limit = AbsoluteTimeWindowLimit::new(
            &[0u32; 1],
            &mut node,
            CmpValue::GreaterEqual(1),
            CmpValue::LessEqual(2),
        );
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        limit.values = &[1u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        limit.values = &[2u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        limit.values = &[3u32; 1];
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
    }

    #[test]
    fn true_count_limit() {
        let mut node = TestValue { is_partition: false, value: true };
        let mut limit = TrueCountLimit::new(&mut node, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);

        limit.reset();

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.evaluate(&ctx), EvalResult::ResetNode);
    }

    #[test]
    fn row_count_limit() {
        let mut node = TestValue { is_partition: false, value: true };
        let mut limit = TrueCountLimit::new(&mut node, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.count, 1);
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.count, 2);
        assert_eq!(limit.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(limit.count, 3);
        assert_eq!(limit.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(limit.count, 0);
    }

    #[test]
    fn limits_chain() {
        let mut node = TestValue { is_partition: false, value: true };
        let mut true_count_limit = TrueCountLimit::new_grouped(&mut node, CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut window_limit = AbsoluteTimeWindowLimit::new(&[0u32; 1], &mut true_count_limit, CmpValue::GreaterEqual(0), CmpValue::LessEqual(1));
        let mut ctx = Context { row_id: 0 };
        {
            assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false));
        }
        {
            assert_eq!(window_limit.evaluate(&ctx), EvalResult::True(false));
        }
        {
            assert_eq!(window_limit.evaluate(&ctx), EvalResult::True(false));
        }
        {
            assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false)); // here is reset
        }
        {
            assert_eq!(window_limit.evaluate(&ctx), EvalResult::False(false));
        }
    }
}

fn main() {}
