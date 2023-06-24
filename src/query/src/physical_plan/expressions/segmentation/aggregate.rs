use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Float16Array;
use arrow::array::Float32Array;
use arrow::array::Float32Builder;
use arrow::array::Float64Array;
use arrow::array::Float64Builder;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::Int8Array;
use arrow::array::PrimitiveBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::record_batch::RecordBatch;
use arrow2::types::f16;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;

use crate::error::Result;
use crate::physical_plan::expressions::segmentation::AggregateFunction;
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
struct AggregateInner<OT, OB>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    last_hash: u64,
    out: OB,
    agg: AggregateFunction<OT>,
}
#[derive(Debug)]
pub struct Aggregate<T, OT, OB>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    inner: Arc<Mutex<AggregateInner<OT, OB>>>,
    predicate: Column,
    arr: PhantomData<OB>,
    typ: PhantomData<T>,
}

impl<T, OT, OB> Aggregate<T, OT, OB>
where
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
    T: Copy,
{
    pub fn try_new(predicate: Column, agg: AggregateFunction<OT>, out: OB) -> Result<Self> {
        let inner = AggregateInner {
            last_hash: 0,
            out,
            agg,
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
            predicate,
            arr: PhantomData,
            typ: Default::default(),
        })
    }
}

macro_rules! gen_agg_int {
    ($ty:ty,$array_ty:ident,$acc_ty:ty,$acc_builder:ty) => {
        impl SegmentationExpr for Aggregate<$ty, $acc_ty, $acc_builder> {
            fn evaluate(&self, record_batch: &RecordBatch, hashes: &[u64]) -> Result<ArrayRef> {
                let mut inner = self.inner.lock().unwrap();
                let arr = self
                    .predicate
                    .evaluate(record_batch)?
                    .into_array(record_batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();
                for (idx, hash) in hashes.iter().enumerate() {
                    if inner.last_hash == 0 {
                        inner.last_hash = *hash;
                    }
                    if *hash != inner.last_hash {
                        inner.last_hash = *hash;
                        let res = inner.agg.result();
                        inner.out.append_value(res);
                        inner.agg.reset();
                    }

                    inner.agg.accumulate(arr.value(idx).into());
                }

                Ok(Arc::new(inner.out.finish()) as ArrayRef)
            }

            fn finalize(&self) -> Result<ArrayRef> {
                let mut inner = self.inner.lock().unwrap();
                let res = inner.agg.result() as $acc_ty;
                inner.out.append_value(res);
                Ok(Arc::new(inner.out.finish()) as ArrayRef)
            }
        }
    };
}

gen_agg_int!(i8, Int8Array, i64, Int64Builder);
gen_agg_int!(i16, Int16Array, i64, Int64Builder);
gen_agg_int!(i32, Int32Array, i64, Int64Builder);
gen_agg_int!(i64, Int64Array, i128, Decimal128Builder);
gen_agg_int!(i128, Decimal128Array, i128, Decimal128Builder);
gen_agg_int!(u8, Int8Array, i64, Int64Builder);
gen_agg_int!(u16, Int16Array, i64, Int64Builder);
gen_agg_int!(u32, Int32Array, i64, Int64Builder);
gen_agg_int!(u64, Int64Array, i128, Decimal128Builder);
gen_agg_int!(u128, Decimal128Array, i128, Decimal128Builder);
gen_agg_int!(f32, Float32Array, f64, Float64Builder);
gen_agg_int!(f64, Float64Array, f64, Float64Builder);
// todo add decimal 256
// gen_agg_int!(i128, Decimal128Array);
// gen_agg_int!(f64, Float32Array);
// gen_agg_int!(f64, Float64Array);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Float16Array;
    use arrow::array::Float32Array;
    use arrow::array::Float32Builder;
    use arrow::array::Float64Array;
    use arrow::array::Float64Builder;
    use arrow::array::Int16Array;
    use arrow::array::Int64Array;
    use arrow::array::Int64Builder;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::hash_utils::create_hashes;

    use crate::physical_plan::expressions::segmentation::aggregate::Aggregate;
    use crate::physical_plan::expressions::segmentation::AggregateFunction;
    use crate::physical_plan::expressions::segmentation::SegmentationExpr;

    #[test]
    fn test_sum() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int16, false),
        ]);
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
        let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3, 1, 2, 3, 1, 2, 3]));
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![col1.clone(), col2.clone()])
                .unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1], &mut random_state, &mut hash_buf).unwrap();
        let mut agg = Aggregate::<i16, i64, _>::try_new(
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Int64Builder::with_capacity(10_000),
        )
        .unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![6, 6])) as ArrayRef;
        assert_eq!(&*res, &*right);
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        let col2: ArrayRef = Arc::new(Int16Array::from(vec![4, 5, 6, 1]));
        hash_buf.clear();
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![col1.clone(), col2.clone()]).unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Int64Array::from(vec![21])) as ArrayRef;
        assert_eq!(&*res, &*right);
        let res = agg.finalize().unwrap();
        let right = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
        assert_eq!(&*res, &*right);
    }

    #[test]
    fn test_sum2() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int16, false),
        ]);
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));
        let col2: ArrayRef = Arc::new(Int16Array::from(vec![1, 2, 3]));
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![col1.clone(), col2.clone()])
                .unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1], &mut random_state, &mut hash_buf).unwrap();
        let mut agg = Aggregate::<i16, i64, _>::try_new(
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Int64Builder::with_capacity(10_000),
        )
        .unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        println!("{:?}", res);
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let col2: ArrayRef = Arc::new(Int16Array::from(vec![4, 5]));
        hash_buf.clear();
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![col1.clone(), col2.clone()]).unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        println!("{:?}", res);
        let res = agg.finalize().unwrap();
        println!("{:?}", res);
    }
    #[test]
    fn test_sum_float() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Float32, false),
        ]);
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
        let col2: ArrayRef =
            Arc::new(Float32Array::from(vec![1., 2., 3., 1., 2., 3., 1., 2., 3.])) as ArrayRef;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![col1.clone(), col2.clone()])
                .unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1], &mut random_state, &mut hash_buf).unwrap();
        let mut agg = Aggregate::<f32, f64, _>::try_new(
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Float64Builder::with_capacity(10_000),
        )
        .unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Float64Array::from(vec![6., 6.])) as ArrayRef;
        assert_eq!(&*res, &*right);

        let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        let col2: ArrayRef = Arc::new(Float32Array::from(vec![4., 5., 6., 1.]));
        hash_buf.clear();
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![col1.clone(), col2.clone()]).unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        let right = Arc::new(Float64Array::from(vec![21.])) as ArrayRef;
        assert_eq!(&*res, &*right);
        let res = agg.finalize().unwrap();
        let right = Arc::new(Float64Array::from(vec![1.])) as ArrayRef;
        assert_eq!(&*res, &*right);
    }
}
