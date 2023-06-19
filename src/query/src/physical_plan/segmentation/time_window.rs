use std::marker::PhantomData;

#[derive(Debug)]
enum TimeWindowName {
    Between,
    From,
    Last,
}

pub trait TimeWindow: Send + Sync {
    fn perform(&self, v: i64) -> bool;
    fn fn_name() -> TimeWindowName;
}

pub struct Between {
    from: i64,
    to: i64,
}

impl TimeWindow for Between {
    fn perform(&self, v: i64) -> bool {
        v >= self.from && v <= self.from
    }

    fn fn_name() -> TimeWindowName {
        TimeWindowName::Between
    }
}
