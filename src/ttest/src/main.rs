macro_rules! test {
    ($($str:tt)+) => {
    vec![
        $(println!("{}",$str.a);)+
]

    }
}
struct A {
    a: usize,
}

fn main() {
    let case = test! {
        {
            a:1
        }
        {
            a:1
        }
    };
}