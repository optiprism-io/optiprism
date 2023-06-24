use std::marker::PhantomData;
use std::sync::Arc;

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
use crate::physical_plan::segmentation2::AggregateFunction;
use crate::physical_plan::segmentation2::SegmentationExpr;

struct Aggregate<T, OT, OB>
where OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone
{
    last_hash: u64,
    out: OB,
    max_out_len: usize,
    agg: AggregateFunction<OT>,
    predicate: Column,
    arr: PhantomData<OB>,
    typ: PhantomData<T>,
}

impl<T, OT, OB> Aggregate<T, OT, OB>
where
    OT: Copy + Num + Bounded + NumCast + PartialOrd + Clone,
    T: Copy,
{
    pub fn try_new(
        max_out_len: usize,
        predicate: Column,
        agg: AggregateFunction<OT>,
        out: OB,
    ) -> Result<Self> {
        Ok(Self {
            last_hash: 0,
            out,
            max_out_len,
            agg,
            predicate,
            arr: PhantomData,
            typ: Default::default(),
        })
    }
}

macro_rules! gen_agg_int {
    ($ty:ty,$array_ty:ident,$acc_ty:ty,$acc_builder:ty ) => {
        impl SegmentationExpr for Aggregate<$ty, $acc_ty, $acc_builder> {
            fn evaluate(
                &mut self,
                record_batch: &RecordBatch,
                hashes: &[u64],
            ) -> Result<Option<ArrayRef>> {
                let arr = self
                    .predicate
                    .evaluate(record_batch)?
                    .into_array(record_batch.num_rows())
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap()
                    .clone();
                for (idx, hash) in hashes.iter().enumerate() {
                    if self.last_hash == 0 {
                        self.last_hash = *hash;
                    }
                    if *hash != self.last_hash {
                        self.last_hash = *hash;
                        self.out.append_value(self.agg.result());
                        self.agg.reset();
                    }

                    self.agg.accumulate(arr.value(idx).into());
                }

                let res = if self.out.len() >= self.max_out_len {
                    Some(Arc::new(self.out.finish()) as ArrayRef)
                } else {
                    None
                };

                Ok(res)
            }

            fn finalize(&mut self) -> Result<Option<ArrayRef>> {
                self.out.append_value(self.agg.result() as $acc_ty);
                if self.out.len() > 0 {
                    Ok(Some(Arc::new(self.out.finish()) as ArrayRef))
                } else {
                    Ok(None)
                }
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

    use crate::physical_plan::segmentation2::aggregate::Aggregate;
    use crate::physical_plan::segmentation2::count::Count;
    use crate::physical_plan::segmentation2::AggregateFunction;
    use crate::physical_plan::segmentation2::SegmentationExpr;

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
            3,
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Int64Builder::with_capacity(10_000),
        )
        .unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        assert_eq!(res, None);

        let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        let col2: ArrayRef = Arc::new(Int16Array::from(vec![4, 5, 6, 1]));
        hash_buf.clear();
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![col1.clone(), col2.clone()]).unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        assert_eq!(
            res,
            Some(Arc::new(Int64Array::from(vec![6, 6, 21])) as ArrayRef)
        );
        let res = agg.finalize().unwrap();
        assert_eq!(res, Some(Arc::new(Int64Array::from(vec![1])) as ArrayRef));
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
            3,
            Column::new_with_schema("col2", &schema).unwrap(),
            AggregateFunction::new_sum(),
            Float64Builder::with_capacity(10_000),
        )
        .unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        assert_eq!(res, None);

        let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        let col2: ArrayRef = Arc::new(Float32Array::from(vec![4., 5., 6., 1.]));
        hash_buf.clear();
        hash_buf.resize(col1.len(), 0);
        create_hashes(&vec![col1.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![col1.clone(), col2.clone()]).unwrap();
        let res = agg.evaluate(&batch, &hash_buf).unwrap();
        assert_eq!(
            res,
            Some(Arc::new(Float64Array::from(vec![6., 6., 21.])) as ArrayRef)
        );
        let res = agg.finalize().unwrap();
        assert_eq!(
            res,
            Some(Arc::new(Float64Array::from(vec![1.])) as ArrayRef)
        );
    }
}
