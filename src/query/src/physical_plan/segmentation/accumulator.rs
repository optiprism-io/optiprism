use std::ops::Add;

use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

#[derive(Debug)]
enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
}

pub trait Primitive: Copy + Num + Bounded + NumCast + PartialOrd + Clone {}

pub trait Accumulator<T>: Send + Sync {
    fn perform(acc: T, v: T) -> T;
    fn fn_name() -> AggregateFunction;
}

#[derive(Debug)]
pub struct Sum;

impl<T> Accumulator<T> for Sum
where T: Add<Output = T>
{
    fn perform(acc: T, v: T) -> T {
        return acc + v;
    }

    fn fn_name() -> AggregateFunction {
        AggregateFunction::Sum
    }
}
