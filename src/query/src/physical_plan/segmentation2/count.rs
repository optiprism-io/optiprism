use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::physical_plan::segmentation2::SegmentationExpr;

pub struct Count {
    last_hash: u64,
    out: Int64Builder,
    max_out_len: usize,
    count: i64,
}

impl Count {
    pub fn new(max_out_len: usize) -> Self {
        Self {
            last_hash: 0,
            out: Int64Builder::with_capacity(10_000),
            max_out_len,
            count: 0,
        }
    }
}

impl SegmentationExpr for Count {
    fn evaluate(
        &mut self,
        _record_batch: &RecordBatch,
        hashes: &[u64],
    ) -> Result<Option<ArrayRef>> {
        for hash in hashes {
            if self.last_hash == 0 {
                self.last_hash = *hash;
            }
            if *hash != self.last_hash {
                self.last_hash = *hash;
                self.out.append_value(self.count);
                self.count = 0;
            }

            self.count += 1;
        }

        let res = if self.out.len() >= self.max_out_len {
            Some(Arc::new(self.out.finish()) as ArrayRef)
        } else {
            None
        };

        Ok(res)
    }

    fn finalize(&mut self) -> Result<Option<ArrayRef>> {
        self.out.append_value(self.count);
        if self.out.len() > 0 {
            Ok(Some(Arc::new(self.out.finish()) as ArrayRef))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::Int64Array;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::hash_utils::create_hashes;

    use crate::physical_plan::segmentation2::count::Count;
    use crate::physical_plan::segmentation2::SegmentationExpr;

    #[test]
    fn it_works() {
        let schema = Schema::new(vec![Field::new("col1", DataType::Int64, false)]);
        let col: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2, 3, 3, 3]));
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![col.clone()]).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(col.len(), 0);
        create_hashes(&vec![col], &mut random_state, &mut hash_buf).unwrap();
        let mut count = Count::new(3);
        let res = count.evaluate(&batch, &hash_buf).unwrap();
        assert_eq!(res, None);

        let col: ArrayRef = Arc::new(Int64Array::from(vec![3, 3, 3, 4]));
        hash_buf.clear();
        hash_buf.resize(col.len(), 0);
        create_hashes(&vec![col.clone()], &mut random_state, &mut hash_buf).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![col.clone()]).unwrap();
        let res = count.evaluate(&batch, &hash_buf).unwrap();

        assert_eq!(
            res,
            Some(Arc::new(Int64Array::from(vec![3, 3, 6])) as ArrayRef)
        );
        let res = count.finalize().unwrap();
        assert_eq!(res, Some(Arc::new(Int64Array::from(vec![1])) as ArrayRef));
    }
}
