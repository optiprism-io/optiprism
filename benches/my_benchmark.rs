use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::any::Any;

fn plus(a: i64) {
    let mut c = 0;
    for _ in 0..1000000 {
        c = c + a;
    }
}

struct V(i64);
fn criterion_plus(c: &mut Criterion) {
    let v: &dyn Any = &V(1);
    c.bench_function("b1", |b| b.iter(|| {
        for i in 0..1000000 {
            black_box(i + v.downcast_ref::<V>().unwrap().0);
        }
    }));
    c.bench_function("b2", |b| b.iter(||
        {
            for i in 0..1000000 {
                black_box(i + 1);
            }
        }
    ));
}

criterion_group!(benches, criterion_plus);
criterion_main!(benches);