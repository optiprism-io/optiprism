use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use datafusion::physical_plan::Accumulator;
use query::physical_plan::expressions::sorted_distinct_count::SortedDistinctCountAccumulator;
use rand::Rng;

fn new_random_array(size: usize, limit: usize) -> ArrayRef {
    let mut vec = Vec::with_capacity(size);

    let mut rng = rand::thread_rng();
    for _ in 0..size {
        vec.push(Some(rng.gen::<i64>() % (limit as i64)));
    }
    vec.sort();

    let array = Int64Array::from(vec);
    Arc::new(array)
}

pub fn sorted_distinct_count_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_distinct_count");

    for size in [10, 20, 40, 100, 200, 400, 1000] {
        let id = BenchmarkId::from_parameter(size);
        let array = new_random_array(size, size / 2);
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(id, &array, |b, array| {
            let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64).unwrap();
            b.iter(move || {
                acc.reset();
                acc.update_batch(&[array.clone()]).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, sorted_distinct_count_bench);
criterion_main!(benches);
