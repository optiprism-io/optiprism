/*pub trait Cmp<T> {
    fn is_true(left: T, right: T) -> bool;
}

pub struct Equal;

impl Cmp<u32> for Equal {
    fn is_true(left: u32, right: u32) -> bool {
        left == right
    }
}

struct NotEqual;

impl Cmp<u32> for NotEqual {
    fn is_true(left: u32, right: u32) -> bool {
        left != right
    }
}*/

use std::marker::PhantomData;

pub trait Cmp<T> {
    fn is_true(left: T, right: T) -> bool;
}

pub struct Equal;

impl<T> Cmp<T> for Equal where T: PartialEq {
    fn is_true(left: T, right: T) -> bool {
        return left == right;
    }
}

pub struct Less;

impl<T> Cmp<T> for Less where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left < right;
    }
}

pub struct LessEqual;

impl<T> Cmp<T> for LessEqual where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left <= right;
    }
}

pub struct Greater;

impl<T> Cmp<T> for Greater where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left > right;
    }
}

pub struct GreaterEqual;

impl<T> Cmp<T> for GreaterEqual where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left >= right;
    }
}

pub struct NotEqual;

impl<T> Cmp<T> for NotEqual where T: PartialEq {
    fn is_true(left: T, right: T) -> bool {
        return left != right;
    }
}