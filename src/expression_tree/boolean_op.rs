pub trait BooleanOp<T> {
    fn perform(left: T, right: T) -> bool;
}

struct And;

/*impl BooleanOp<Option<bool>> for And {
    fn perform(left: Option<bool>, right: Option<bool>) -> bool {
        return left && right;
    }
}*/

impl BooleanOp<bool> for And {
    fn perform(left: bool, right: bool) -> bool {
        return left && right;
    }
}

struct Or;

impl BooleanOp<bool> for Or {
    fn perform(left: bool, right: bool) -> bool {
        return left || right;
    }
}

pub struct Equal;

impl<T> BooleanOp<T> for Equal where T:PartialEq{
    fn perform(left: T, right: T) -> bool {
        return left == right;
    }
}
