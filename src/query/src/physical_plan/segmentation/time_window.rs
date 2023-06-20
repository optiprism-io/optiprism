use std::fmt::Debug;
use std::fmt::Formatter;
use std::marker::PhantomData;

use dyn_clone::DynClone;

#[derive(Debug)]
enum TimeWindowName {
    Between,
    From,
    Last,
}

pub trait TimeWindow: Send + Sync + Debug + DynClone {
    fn check_bounds(&self, v: i64) -> bool;
    fn reset(&mut self);
    fn fn_name(&self) -> TimeWindowName;
}

#[derive(Clone)]
pub struct Between {
    pub from: i64,
    pub to: i64,
}

impl TimeWindow for Between {
    fn check_bounds(&self, v: i64) -> bool {
        v >= self.from && v <= self.to
    }

    fn reset(&mut self) {}

    fn fn_name(&self) -> TimeWindowName {
        TimeWindowName::Between
    }
}

impl Debug for Between {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Between")
    }
}
