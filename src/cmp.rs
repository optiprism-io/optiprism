pub trait Cmp<T: Copy> {
    fn is_true(left: T, right: T) -> bool;
}

pub struct Equal;

impl<T: Copy> Cmp<T> for Equal where T: PartialEq + Copy {
    fn is_true(left: T, right: T) -> bool {
        return left == right;
    }
}

pub struct Less;

impl<T: Copy> Cmp<T> for Less where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left < right;
    }
}

pub struct LessEqual;

impl<T: Copy> Cmp<T> for LessEqual where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left <= right;
    }
}

pub struct Greater;

impl<T: Copy> Cmp<T> for Greater where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left > right;
    }
}

pub struct GreaterEqual;

impl<T: Copy> Cmp<T> for GreaterEqual where T: PartialOrd {
    fn is_true(left: T, right: T) -> bool {
        return left >= right;
    }
}

pub struct NotEqual;

impl<T: Copy> Cmp<T> for NotEqual where T: PartialEq {
    fn is_true(left: T, right: T) -> bool {
        return left != right;
    }
}