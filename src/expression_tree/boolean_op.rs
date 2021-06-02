use datafusion::logical_plan::Operator;

pub trait BooleanOp<T> {
    fn perform(left: T, right: T) -> bool;
    fn op() -> Operator;
}

pub struct And;

impl BooleanOp<bool> for And {
    fn perform(left: bool, right: bool) -> bool {
        return left && right;
    }

    fn op() -> Operator {
        Operator::And
    }
}

pub struct Or;

impl BooleanOp<bool> for Or {
    fn perform(left: bool, right: bool) -> bool {
        return left || right;
    }

    fn op() -> Operator {
        Operator::Or
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


pub struct Eq;

impl<T> BooleanOp<T> for Eq where T: PartialEq {
    fn perform(left: T, right: T) -> bool {
        return left == right;
    }

    fn op() -> Operator {
        Operator::Eq
    }
}

pub struct Gt;

impl<T> BooleanOp<T> for Gt where T: Ord {
    fn perform(left: T, right: T) -> bool {
        return left > right;
    }

    fn op() -> Operator {
        Operator::Gt
    }
}

pub struct Lt;

impl<T> BooleanOp<T> for Lt where T: Ord {
    fn perform(left: T, right: T) -> bool {
        return left < right;
    }

    fn op() -> Operator {
        Operator::Lt
    }
}