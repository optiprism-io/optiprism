pub trait BooleanOp<T> {
    fn perform(left: T, right: T) -> bool;
}

pub struct And;

impl BooleanOp<bool> for And {
    fn perform(left: bool, right: bool) -> bool {
        return left && right;
    }
}

pub struct Or;

impl BooleanOp<bool> for Or {
    fn perform(left: bool, right: bool) -> bool {
        return left || right;
    }
}

impl<'a> BooleanOp<Option<&'a str>> for Or {
    fn perform(left: Option<&'a str>, right: Option<&'a str>) -> bool {
        return left == right;
    }
}


pub struct Eq;

impl<T> BooleanOp<T> for Eq where T: PartialEq {
    fn perform(left: T, right: T) -> bool {
        return left == right;
    }
}

pub struct Gt;

impl<T> BooleanOp<T> for Gt where T: Ord {
    fn perform(left: T, right: T) -> bool {
        return left > right;
    }
}

pub struct Lt;

impl<T> BooleanOp<T> for Lt where T: Ord {
    fn perform(left: T, right: T) -> bool {
        return left < right;
    }
}