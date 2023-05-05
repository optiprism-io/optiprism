use std::fmt::{Debug, Display};
use std::ops::BitAnd;
use datafusion_expr::Operator;

pub trait BooleanOp<T>: Send + Sync + Display + Debug {
    fn perform(left: T, right: T) -> bool;
    fn op() -> Operator;
}

#[derive(Debug)]
pub struct BooleanAnd;

impl<T> BooleanOp<T> for BooleanAnd
{
    fn perform(left: T, right: T) -> bool {
        return left && right;
    }

    fn op() -> Operator {
        Operator::And
    }
}

impl Display for BooleanAnd {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "And")
    }
}

#[derive(Debug)]
pub struct BooleanOr;

impl<T> BooleanOp<T> for BooleanOr
{
    fn perform(left: T, right: T) -> bool {
        return left || right;
    }

    fn op() -> Operator {
        Operator::Or
    }
}

impl Display for BooleanOr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Or")
    }
}

#[derive(Debug)]
pub struct BooleanEq;

impl<T> BooleanOp<T> for BooleanEq
    where
        T: PartialEq,
{
    fn perform(left: T, right: T) -> bool {
        return left == right;
    }

    fn op() -> Operator {
        Operator::Eq
    }
}

impl Display for BooleanEq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Eq")
    }
}

#[derive(Debug)]
pub struct BooleanGt;

impl<T> BooleanOp<T> for BooleanGt
    where
        T: Ord,
{
    fn perform(left: T, right: T) -> bool {
        return left > right;
    }

    fn op() -> Operator {
        Operator::Gt
    }
}

impl Display for BooleanGt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Gt")
    }
}

#[derive(Debug)]
pub struct BooleanLt;

impl<T> BooleanOp<T> for BooleanLt
    where
        T: Ord,
{
    fn perform(left: T, right: T) -> bool {
        return left < right;
    }

    fn op() -> Operator {
        Operator::Lt
    }
}

impl Display for BooleanLt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Lt")
    }
}
