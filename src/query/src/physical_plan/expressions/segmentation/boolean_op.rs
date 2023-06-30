use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::BitAnd;

#[derive(Debug, Clone)]
pub enum Operator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

pub trait ComparisonOp<T>: Send + Sync + Debug {
    fn perform(left: T, right: T) -> bool;
    fn op() -> Operator;
}

#[derive(Debug)]
pub struct Eq;

impl<T> ComparisonOp<T> for Eq
    where T: PartialEq
{
    fn perform(left: T, right: T) -> bool {
        return left == right;
    }

    fn op() -> Operator {
        Operator::Eq
    }
}

impl Display for Eq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Eq")
    }
}

#[derive(Debug)]
pub struct NotEq;

impl<T> ComparisonOp<T> for NotEq
    where T: PartialEq
{
    fn perform(left: T, right: T) -> bool {
        return left != right;
    }

    fn op() -> Operator {
        Operator::NotEq
    }
}

impl Display for NotEq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NotEq")
    }
}

#[derive(Debug)]
pub struct Gt;

impl<T> ComparisonOp<T> for Gt
    where T: Ord
{
    fn perform(left: T, right: T) -> bool {
        return left > right;
    }

    fn op() -> Operator {
        Operator::Gt
    }
}

impl Display for Gt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Gt")
    }
}

#[derive(Debug)]
pub struct Lt;

impl<T> ComparisonOp<T> for Lt
    where T: Ord
{
    fn perform(left: T, right: T) -> bool {
        return left < right;
    }

    fn op() -> Operator {
        Operator::Lt
    }
}

impl Display for Lt {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Lt")
    }
}
