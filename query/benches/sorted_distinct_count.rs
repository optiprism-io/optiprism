use std::collections::HashSet;
use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};
use datafusion::physical_plan::Accumulator;
use query::physical_plan::expressions::sorted_distinct_count::SortedDistinctCountAccumulator;
use rand::Rng;

fn new_random_array(size: usize, limit: usize) -> (ArrayRef, usize) {
    let mut vec = Vec::with_capacity(size);

    let mut rng = rand::thread_rng();
    for _ in 0..size {
        vec.push(Some(rng.gen::<i64>() % (limit as i64)));
    }
    vec.sort();

    let distinct = vec.iter()
        .flat_map(|x| x)
        .collect::<HashSet<_>>()
        .len();

    let array = Int64Array::from(vec);
    (Arc::new(array), distinct)
}

pub fn sorted_distinct_count_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_distinct_count");

    for size in [10, 20, 40, 100, 200, 400, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        let id = BenchmarkId::from_parameter(size);
        group.bench_with_input(id, &size, |b, &size| {
            let (array, distinct_count) = new_random_array(size, size / 2);
            b.iter(|| {
                let mut acc = SortedDistinctCountAccumulator::try_new(&DataType::Int64).unwrap();
                acc.update_batch(&[array.clone()]).unwrap();
                assert_eq!(acc.count() as usize, distinct_count);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, sorted_distinct_count_bench);
criterion_main!(benches);
