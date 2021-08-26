use datafusion::logical_plan::Operator;
use std::fmt::{Debug, Display};

pub trait BooleanOp<T>: Send + Sync + Display + Debug {
    fn perform(left: T, right: T) -> bool;
    fn op() -> Operator;
}

#[derive(Debug)]
pub struct And;

impl BooleanOp<bool> for And {
    fn perform(left: bool, right: bool) -> bool {
        return left && right;
    }

    fn op() -> Operator {
        Operator::And
    }
}

impl std::fmt::Display for And {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "And")
    }
}

#[derive(Debug)]
pub struct Or;

impl BooleanOp<bool> for Or {
    fn perform(left: bool, right: bool) -> bool {
        return left || right;
    }

    fn op() -> Operator {
        Operator::Or
    }
}

impl std::fmt::Display for Or {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OR")
    }
}

impl<'a> BooleanOp<Option<&'a str>> for Or {
    fn perform(left: Option<&'a str>, right: Option<&'a str>) -> bool {
        return left == right;
    }

    fn op() -> Operator {
        Operator::Or
    }
}

#[derive(Debug)]
pub struct Eq;

impl<T> BooleanOp<T> for Eq
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

impl std::fmt::Display for Eq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Eq")
    }
}

#[derive(Debug)]
pub struct Gt;

impl<T> BooleanOp<T> for Gt
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

impl std::fmt::Display for Gt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Gt")
    }
}

#[derive(Debug)]
pub struct Lt;

impl<T> BooleanOp<T> for Lt
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

impl std::fmt::Display for Lt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Lt")
    }
}
