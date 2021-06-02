use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::any::Any;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{ArrayRef, Int8Array, Int64Array};
use datafusion::physical_plan::expressions::{Column, Literal, BinaryExpr};
use datafusion::scalar::ScalarValue;
use datafusion::logical_plan::Operator;
use exprtree::expression_tree::count::Count;
use exprtree::expression_tree::expr::Expr;
use exprtree::expression_tree::boolean_op::{Gt, Eq, Lt, And};
use exprtree::expression_tree::iterative_count_op::IterativeCountOp;
use exprtree::expression_tree::value_op::ValueOp;
use exprtree::expression_tree::binary_op::BinaryOp;

fn create_batch<'a>(num_cols: usize, size: usize) -> Box<RecordBatch> {
    let mut fields: Vec<Field> = vec![];

    for i in 0..num_cols {
        fields.push(Field::new(i.to_string().as_str(), DataType::Int64, true))
    }

    let schema = Schema::new(fields);
    let mut cols: Vec<ArrayRef> = vec![];


    for i in 0..num_cols {
        let mut c: Vec<Option<i64>> = vec![];
        for j in 0..size {
            if i == num_cols - 1 {
                c.push(Some(j as i64));
            } else {
                c.push(Some(1i64));
            }
        }
        cols.push(Arc::new(Int64Array::from(c)));
    }

    let a = RecordBatch::try_new(
        Arc::new(schema),
        cols,
    ).unwrap();

    Box::new(a)
}

fn criterion_count(c: &mut Criterion) {
    let batch = create_batch(2, 1000);
    let distance = vec![1i64, 500i64, 999i64];

    let batch_exprs: Vec<Box<Count>> = distance.iter().map(|x| {
        let al = Column::new("0");
        let ar = Literal::new(ScalarValue::Int64(Some(1)));
        let aop = BinaryExpr::new(Arc::new(al), Operator::Eq, Arc::new(ar));

        let bl = Column::new("1");
        let br = Literal::new(ScalarValue::Int64(Some(*x)));
        let bop = BinaryExpr::new(Arc::new(bl), Operator::Eq, Arc::new(br));

        let op = BinaryExpr::new(Arc::new(aop), Operator::And, Arc::new(bop));
        Box::new(Count::new(Arc::new(op)))
    }).collect();


    let iter_exprs: Vec<Box<IterativeCountOp<Eq>>> = distance.iter().map(|x| {
        let left = ValueOp::<Option<i64>, Eq>::new(0, Some(1));
        let right = ValueOp::<Option<i64>, Eq>::new(1, Some(*x));
        let expr = BinaryOp::<bool, And>::new(Box::new(left), Box::new(right));
        Box::new(IterativeCountOp::<Eq>::new(
            Box::new(expr),
            1,
        ))
    }).collect();

    let mut group = c.benchmark_group("count");

    for (i, d) in distance.iter().enumerate() {
        group.bench_with_input(BenchmarkId::new("Batch", format!("{}", d)), &i, |b, i| {
            b.iter(|| batch_exprs[*i].evaluate(&batch, 0))
        });

        group.bench_with_input(BenchmarkId::new("Iter", format!("{}", d)), &i, |b, i| {
            b.iter(|| iter_exprs[*i].evaluate(&batch, 0))
        });
    }
}

criterion_group!(benches, criterion_count);
criterion_main!(benches);