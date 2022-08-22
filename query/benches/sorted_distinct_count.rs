use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion_core::physical_plan::expressions::DistinctCount;
use datafusion_core::physical_plan::{Accumulator, AggregateExpr};
use query::physical_plan::expressions::sorted_distinct_count::SortedDistinctCountAccumulator;
use rand::Rng;
use std::sync::Arc;

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

    let arrays = [10, 20, 40, 100, 200, 400, 1000]
        .into_iter()
        .map(|size| new_random_array(size, size / 2))
        .collect::<Vec<_>>();

    for array in arrays {
        let size = array.len() as u64;
        group.throughput(Throughput::Elements(size));
        let id = BenchmarkId::from_parameter(size);
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

pub fn distinct_count_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct_count");

    let arrays = [10, 20, 40, 100, 200, 400, 1000]
        .into_iter()
        .map(|size| new_random_array(size, size / 2))
        .collect::<Vec<_>>();

    for array in arrays {
        let size = array.len() as u64;
        group.throughput(Throughput::Elements(size));
        let id = BenchmarkId::from_parameter(size);
        group.bench_with_input(id, &array, |b, array| {
            let dc = DistinctCount::new(
                vec![],
                vec![],
                "distinct count".to_string(),
                DataType::Int64,
            );
            let mut acc = dc.create_accumulator().unwrap();
            b.iter(move || {
                acc.update_batch(&[array.clone()]).unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, sorted_distinct_count_bench, distinct_count_bench);
criterion_main!(benches);
