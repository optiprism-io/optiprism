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

pub trait BooleanOp<T>: Send + Sync {
    fn perform(left: T, right: T) -> bool;
    fn op() -> Operator;
}

#[derive(Debug)]
pub struct BooleanEq;

impl<T> BooleanOp<T> for BooleanEq
where T: PartialEq
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
pub struct BooleanNotEq;

impl<T> BooleanOp<T> for BooleanNotEq
where T: PartialEq
{
    fn perform(left: T, right: T) -> bool {
        return left != right;
    }

    fn op() -> Operator {
        Operator::NotEq
    }
}

impl Display for BooleanNotEq {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NotEq")
    }
}

#[derive(Debug)]
pub struct BooleanGt;

impl<T> BooleanOp<T> for BooleanGt
where T: Ord
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
where T: Ord
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
