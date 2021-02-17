pub trait Cmp<T> {
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
}

pub fn a() {

}