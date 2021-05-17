use std::convert::{From, TryInto, TryFrom};
use arrow::array::{Array, ArrayRef,Int8Array};
use datafusion::scalar::ScalarValue;

pub trait Cmp<T> {
    fn is_true(row_id: usize, left: &ArrayRef, right: T) -> bool;
}

pub struct Equal;

impl Cmp<i8> for Equal {
    fn is_true(row_id: usize, left: &ArrayRef, right: i8) -> bool {
        return left.as_any().downcast_ref::<Int8Array>().unwrap().value(row_id) == right;
    }
}
/*
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

#[derive(Debug, Copy, Clone)]
pub enum CmpValue<T> {
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
            CmpValue::Equal(right) => Equal::is_true(left, right),
            CmpValue::NotEqual(right) => NotEqual::is_true(left, right),
            CmpValue::Less(right) => Less::is_true(left, right),
            CmpValue::LessEqual(right) => LessEqual::is_true(left, right),
            CmpValue::Greater(right) => GreaterEqual::is_true(left, right),
            CmpValue::GreaterEqual(right) => GreaterEqual::is_true(left, right),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cmp() {
        assert_eq!(<Equal as Cmp<u32>>::is_true(1, 2), false);
        assert_eq!(<Equal as Cmp<u32>>::is_true(1, 1), true);

        assert_eq!(<Less as Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<Less as Cmp<u32>>::is_true(1, 1), false);

        assert_eq!(<LessEqual as Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<LessEqual as Cmp<u32>>::is_true(2, 2), true);
        assert_eq!(<LessEqual as Cmp<u32>>::is_true(3, 2), false);

        assert_eq!(<Greater as Cmp<u32>>::is_true(2, 1), true);
        assert_eq!(<Greater as Cmp<u32>>::is_true(1, 1), false);

        assert_eq!(<GreaterEqual as Cmp<u32>>::is_true(2, 1), true);
        assert_eq!(<GreaterEqual as Cmp<u32>>::is_true(2, 2), true);
        assert_eq!(<GreaterEqual as Cmp<u32>>::is_true(2, 3), false);

        assert_eq!(<NotEqual as Cmp<u32>>::is_true(1, 2), true);
        assert_eq!(<NotEqual as Cmp<u32>>::is_true(1, 1), false);
    }
}
*/
