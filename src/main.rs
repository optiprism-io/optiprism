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

trait SpanLimit {
    fn check(&mut self, ctx: &Context) -> LimitCheckResult;
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
    pub fn new(values: &'a [T], from: CmpValue<i64>, to: CmpValue<i64>) -> Self {
        let mut obj = AbsoluteTimeWindowLimit {
            from: from.to(),
            to: to.to(),
            values,
        };

        obj
    }
}

struct AbsoluteTimeWindowLimit<'a, T> {
    from: CmpValue<T>,
    to: CmpValue<T>,
    values: &'a [T],
}

impl<'a, T> Limit for AbsoluteTimeWindowLimit<'a, T> where T: Copy + PartialEq + PartialOrd {
    fn check(&mut self, ctx: &Context, _: bool) -> LimitCheckResult {
        let ts = self.values[ctx.row_id];


        let left = self.from.cmp(ts);
        let right = self.to.cmp(ts);
        if left && right {
            return LimitCheckResult::True;
        }


        if self.to.is_set() && !right {
            return LimitCheckResult::ResetNode;
        }

        return LimitCheckResult::False;
    }

    fn reset(&mut self) {}
}

struct TrueCountLimit {
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
}

impl TrueCountLimit {
    pub fn new(from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        TrueCountLimit {
            from,
            to,
            count: 0,
        }
    }
}

impl Limit for TrueCountLimit {
    fn check(&mut self, _: &Context, matched: bool) -> LimitCheckResult {
        if matched {
            self.count += 1;
        }

        let left = self.from.cmp(self.count);
        let right = self.to.cmp(self.count);
        if left && right {
            return LimitCheckResult::True;
        }


        if self.to.is_set() && !right {
            return LimitCheckResult::ResetNode;
        }

        return LimitCheckResult::False;
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

struct RowLimit {
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
}

impl RowLimit {
    pub fn new(from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        RowLimit {
            from,
            to,
            count: 0,
        }
    }
}

impl Limit for RowLimit {
    fn check(&mut self, _: &Context, _: bool) -> LimitCheckResult {
        self.count += 1;
        let left = self.from.cmp(self.count);
        let right = self.to.cmp(self.count);
        if left && right {
            return LimitCheckResult::True;
        }


        if self.to.is_set() && !right {
            return LimitCheckResult::ResetNode;
        }

        return LimitCheckResult::False;
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

struct Limits<'a> {
    limits: Vec<&'a mut dyn Limit>,
}

impl<'a> Limits<'a> {
    pub fn new(limits: Vec<&'a mut dyn Limit>) -> Self {
        Limits { limits }
    }

    pub fn check(&mut self, ctx: &Context, eval_result: EvalResult) -> LimitCheckResult {
        let mut matched: bool = false;
        match eval_result {
            EvalResult::True(true) | EvalResult::False(true) => return LimitCheckResult::True,
            EvalResult::True(false) => matched = true,
            _ => {}
        }

        let mut ret = LimitCheckResult::True;

        for l in self.limits.iter_mut() {
            match l.check(ctx, matched) {
                LimitCheckResult::False => ret = LimitCheckResult::False,
                LimitCheckResult::ResetNode => {
                    return LimitCheckResult::ResetNode;
                }
                _ => {}
            }
        }

        return ret;
    }
    fn reset(&mut self) {
        for l in self.limits.iter_mut() {
            l.reset();
        }
    }
}

struct SpanLimits<'a> {
    limits: Vec<&'a mut dyn SpanLimit>,
}

impl<'a> SpanLimits<'a> {
    pub fn new(limits: Vec<&'a mut dyn SpanLimit>) -> Self {
        SpanLimits { limits }
    }

    pub fn check(&mut self, ctx: &Context) -> LimitCheckResult {
        let mut ret = LimitCheckResult::True;
        for l in self.limits.iter_mut() {
            match l.check(ctx) {
                LimitCheckResult::False => ret = LimitCheckResult::False,
                LimitCheckResult::ResetNode => {
                    return LimitCheckResult::ResetNode;
                }
                _ => {}
            }
        }

        return ret;
    }
    fn reset(&mut self) {
        for l in self.limits.iter_mut() {
            l.reset();
        }
    }
}

struct RowCountSpanLimit {
    from: CmpValue<u32>,
    to: CmpValue<u32>,
    count: u32,
}

impl RowCountSpanLimit {
    pub fn new(from: CmpValue<u32>, to: CmpValue<u32>) -> Self {
        RowCountSpanLimit {
            from,
            to,
            count: 0,
        }
    }
}

impl SpanLimit for RowCountSpanLimit {
    fn check(&mut self, _: &Context) -> LimitCheckResult {
        self.count += 1;
        let left = self.from.cmp(self.count);
        let right = self.to.cmp(self.count);
        if left && right {
            return LimitCheckResult::True;
        }

        if self.to.is_set() && !right {
            return LimitCheckResult::ResetNode;
        }

        return LimitCheckResult::False;
    }

    fn reset(&mut self) {
        self.count = 0;
    }
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
                    return self.check_limits(ctx, EvalResult::False(stateful));
                }
            }
        }

        if is_stateful {
            self.state = NodeState::True;
            return self.check_limits(ctx, EvalResult::True(true));
        }

        self.check_limits(ctx, EvalResult::True(false))
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
    fn check_limits(&mut self, ctx: &Context, eval_result: EvalResult) -> EvalResult {
        if let Some(limits) = &mut self.limits {
            return match limits.check(ctx, eval_result) {
                LimitCheckResult::True => eval_result,
                LimitCheckResult::False => EvalResult::False(false),
                LimitCheckResult::ResetNode => {
                    self.reset();
                    EvalResult::False(false)
                }
            };
        }

        eval_result
    }
}

struct Or<'a> {
    state: NodeState,
    children: Vec<&'a mut dyn Node>,
    limits: Option<&'a mut Limits<'a>>,
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
                    return self.check_limits(ctx, EvalResult::True(stateful));
                }
                EvalResult::False(stateful) => {
                    if !stateful {
                        is_stateful = false;
                    }
                }
            }
        }
        self.check_limits(ctx, EvalResult::False(is_stateful))
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

impl<'a> Or<'a> {
    fn check_limits(&mut self, ctx: &Context, eval_result: EvalResult) -> EvalResult {
        if let Some(limits) = &mut self.limits {
            return match limits.check(ctx, eval_result) {
                LimitCheckResult::True => eval_result,
                LimitCheckResult::False => EvalResult::False(false),
                LimitCheckResult::ResetNode => {
                    self.reset();
                    EvalResult::False(false)
                }
            };
        }

        eval_result
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
    span_limits: Option<&'a mut SpanLimits<'a>>,
    state: SequenceState,
}

impl<'a> Sequence<'a> {
    fn new(left: &'a mut dyn Node, right: &'a mut dyn Node) -> Self {
        Sequence {
            left_node: left,
            right_node: right,
            span_limits: None,
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
    use crate::Limits;

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
            limits: None,
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
            limits: None,
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
            limits: None,
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
            limits: None,
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
            limits: None,
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
            limits: None,
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
        let mut limit = AbsoluteTimeWindowLimit::new(&[0u32; 1], CmpValue::GreaterEqual(1), CmpValue::LessEqual(2));
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.check(&ctx, false), LimitCheckResult::False);
        limit.values = &[1u32; 1];
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::True);
        limit.values = &[2u32; 1];
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::True);
        limit.values = &[3u32; 1];
        assert_eq!(limit.check(&ctx, false), LimitCheckResult::ResetNode);
    }

    #[test]
    fn true_count_limit() {
        let mut limit = TrueCountLimit::new(CmpValue::GreaterEqual(1), CmpValue::LessEqual(2));
        let mut ctx = Context { row_id: 0 };

        assert_eq!(limit.check(&ctx, false), LimitCheckResult::False);
        assert_eq!(limit.check(&ctx, true), LimitCheckResult::True);
        assert_eq!(limit.check(&ctx, true), LimitCheckResult::True);
        assert_eq!(limit.check(&ctx, true), LimitCheckResult::ResetNode);
    }

    #[test]
    fn limits() {
        let mut window_limit = AbsoluteTimeWindowLimit::new(&[0u32; 1], CmpValue::GreaterEqual(0), CmpValue::LessEqual(1));
        let mut true_count_limit = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut ctx = Context { row_id: 0 };
        {
            let mut limits = Limits::new(vec![&mut window_limit, &mut true_count_limit]);
            assert_eq!(limits.check(&ctx, EvalResult::True(false)), LimitCheckResult::False);
            assert_eq!(true_count_limit.count, 1);
        }
        {
            let mut limits = Limits::new(vec![&mut window_limit, &mut true_count_limit]);
            assert_eq!(limits.check(&ctx, EvalResult::False(false)), LimitCheckResult::False);
            assert_eq!(true_count_limit.count, 1);
        }
        {
            let mut limits = Limits::new(vec![&mut window_limit, &mut true_count_limit]);
            assert_eq!(limits.check(&ctx, EvalResult::True(false)), LimitCheckResult::True);
            assert_eq!(true_count_limit.count, 2);
        }
        {
            let mut limits = Limits::new(vec![&mut window_limit, &mut true_count_limit]);
            assert_eq!(limits.check(&ctx, EvalResult::True(false)), LimitCheckResult::True);
            assert_eq!(true_count_limit.count, 3);
        }
        {
            let mut limits = Limits::new(vec![&mut window_limit, &mut true_count_limit]);
            assert_eq!(limits.check(&ctx, EvalResult::True(false)), LimitCheckResult::ResetNode);
            assert_eq!(true_count_limit.count, 4);
        }
    }

    #[test]
    fn and_limits() {
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

        let mut true_count_limit = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut limits = Limits::new(vec![&mut true_count_limit]);

        let mut q = And {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: Some(&mut limits),
        };

        let ctx = Context::default();
        // 1st iteration
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        // 2nd iteration, after node reset
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
    }

    #[test]
    fn or_limits() {
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

        let mut true_count_limit = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut limits = Limits::new(vec![&mut true_count_limit]);

        let mut q = Or {
            state: NodeState::None,
            children: vec![
                &mut a,
                &mut b,
            ],
            limits: Some(&mut limits),
        };

        let ctx = Context::default();
        // 1st iteration
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        // 2nd iteration, after node reset
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::True(false));
        assert_eq!(q.evaluate(&ctx), EvalResult::False(false));
    }

    /*#[test]
    fn sequence_limits() {
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
            right: 1,
        };
        let mut c = ScalarValue::<u32, cmp::Equal> {
            c: PhantomData,
            state: NodeState::None,
            is_partition: false,
            left: 1,
            right: 1,
        };

        let mut true_count_limit = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut limits = Limits::new(vec![&mut true_count_limit]);

        let mut true_count_limit2 = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut limits2 = Limits::new(vec![&mut true_count_limit2]);

        let mut true_count_limit3 = TrueCountLimit::new(CmpValue::GreaterEqual(2), CmpValue::LessEqual(3));
        let mut limits3 = Limits::new(vec![&mut true_count_limit3]);

        let mut s = Sequence {
            current_node_idx: 0,
            nodes: vec![
                SequenceNode { node: &mut a, limits: Some(&mut limits) },
                SequenceNode { node: &mut b, limits: Some(&mut limits2) },
                SequenceNode { node: &mut c, limits: Some(&mut limits3) },
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
    }*/
}

fn main() {}
