use std::fmt::Debug;
use std::marker::PhantomData;

use arrow::array::BooleanArray;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions::Column;

use crate::error::Result;
use crate::physical_plan::segmentation::aggregate::Accumulator;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use crate::physical_plan::segmentation::time_window::TimeWindow;

pub trait Expr {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<usize>>;
    fn finalize(&mut self) -> Result<Vec<usize>>;
}

#[derive(Debug)]
pub struct Aggregate<T, Acc, Op>
where T: Debug
{
    ts_col: Column,
    predicate: PhysicalExprRef,
    acc: PhantomData<Acc>,
    op: PhantomData<Op>,
    right: T,
    left_col: Column,
    time_window: Box<dyn TimeWindow>,
}

impl<T, Acc, Op> Aggregate<T, Acc, Op>
where
    T: Default,
    Acc: Accumulator<T>,
    Op: BooleanOp<T>,
{
    pub fn try_new(ts: Column, predicate: PhysicalExprRef, left_col: Column) -> Result<Self> {
        let res = Self {
            ts_col: ts,
            predicate,
            acc: Default::default(),
            op: Default::default(),
            right: T::default(),
            time_window: Default::default(),
            left_col,
        };

        Ok(res)
    }
}

// Batch for state
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub ts: TimestampMillisecondArray,
    pub predicate: Vec<BooleanArray>,
    pub batch: &'a RecordBatch,
}

// Span is a span of rows that are in the same partition
#[derive(Debug, Clone)]
pub struct Span<'a> {
    id: usize,                // # of span
    offset: usize, // offset of the span. Used to skip rows from record batch. See PartitionedState
    len: usize,    // length of the span
    batches: &'a [Batch<'a>], // one or more batches with span
    row_id: usize, // current row id
}

impl<'a> Span<'a> {
    pub fn new(id: usize, offset: usize, len: usize, batches: &'a [Batch]) -> Self {
        Self {
            id,
            offset,
            len,
            batches,
            row_id: 0,
        }
    }

    #[inline]
    pub fn abs_row_id(&self) -> (usize, usize) {
        let mut batch_id = 0;
        let mut idx = self.row_id + self.offset;
        // go over the batches and find the batch that contains the row
        for batch in self.batches {
            if idx < batch.batch.num_rows() {
                break;
            }
            idx -= batch.batch.num_rows();
            batch_id += 1;
        }
        (batch_id, idx)
    }

    // get ts value of current row
    #[inline]
    pub fn ts_value(&self) -> i64 {
        // calculate batch id and row id
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].ts.value(idx)
    }

    // go to next row
    #[inline]
    pub fn next_row(&mut self) -> bool {
        if self.row_id == self.len - 1 {
            return false;
        }
        self.row_id += 1;

        true
    }
}

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

impl<T, Op, Acc> Expr for Aggregate<T, Op, Acc> {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<usize>> {
        // evaluate each batch, e.g. create batch state
        let batches = record_batches
            .iter()
            .map(|b| self.evaluate_batch(b))
            .collect::<Result<Vec<_>>>()?;

        self.cur_span = 0;

        // if skip is set, we need to skip the first span
        let spans = if skip > 0 {
            let spans = [vec![skip], spans].concat();
            self.next_span(&batches, &spans);
            spans
        } else {
            spans
        };

        let mut results = vec![];
    }

    fn finalize(&mut self) -> Result<Vec<usize>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;

    use crate::physical_plan::segmentation::aggregate::Accumulator;
    use crate::physical_plan::segmentation::aggregate::Sum;
    use crate::physical_plan::segmentation::boolean_op::BooleanLt;
    use crate::physical_plan::segmentation::boolean_op::Operator;
    // use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::sum::{Aggregate, Expr};
    use crate::physical_plan::segmentation::time_window::Between;

    #[test]
    fn test_sum() {
        let data = r#"
| user_id | ts  | event | v  |
|---------|-----|-------|--------|
| 0       | 1   | e1    | 1      |
| 0       | 2   | e2    | 1      |
| 0       | 3   | e3    | 1      |
| 0       | 4   | e1    | 1      |

| 0       | 1   | e1    | 1      |
| 0       | 2   | e2    | 1      |
| 0       | 3   | e3    | 1      |
| 1       | 5   | e1    | 1      |
| 1       | 6   | e3    | 1      |
| 1       | 7   | e1    | 1      |
| 1       | 8   | e2    | 1      |
| 2       | 9   | e1    | 1      |
| 2       | 10  | e2    | 1      |

| 2       | 11  | e3    | 1      |
| 2       | 12  | e3    | 1      |
| 2       | 13  | e3    | 1      |
| 2       | 14  | e3    | 1      |
| 3       | 15  | e1    | 1      |
| 3       | 16  | e2    | 1      |
| 3       | 17  | e3    | 1      |
| 4       | 18  | e1    | 1      |
| 4       | 19  | e2    | 1      |

| 4       | 20  | e3    | 1      |

| 5       | 21  | e1    | 1      |

| 5       | 22  | e2    | 1      |

| 6       | 23  | e1    | 1      |
| 6       | 24  | e3    | 1      |
| 6       | 25  | e3    | 1      |

| 7       | 26  | e1    | 1      |
| 7       | 27  | e2    | 1      |
| 7       | 28  | e3    | 1      |

| 8       | 29  | e1    | 1      |
| 8       | 30  | e2    | 1      |
| 8       | 31  | e3    | 1      |
"#;

        let fields = vec![
            arrow2::datatypes::Field::new("user_id", arrow2::datatypes::DataType::Int64, false),
            arrow2::datatypes::Field::new(
                "ts",
                arrow2::datatypes::DataType::Timestamp(
                    arrow2::datatypes::TimeUnit::Millisecond,
                    None,
                ),
                false,
            ),
            arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
            arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
        ];

        let (arrs, schema) = {
            // todo change to arrow1

            let res = parse_markdown_table(data, &fields).unwrap();

            let (arrs, fields) = res
                .into_iter()
                .zip(fields.clone())
                .map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap())
                .unzip();

            let schema = Arc::new(Schema::new(fields)) as SchemaRef;

            (arrs, schema)
        };

        let predicate = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), datafusion_expr::Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };
        let ts_col = Column::new_with_schema("ts", &schema).unwrap();
        let left_col = Column::new_with_schema("v", &schema).unwrap();
        let mut agg =
            Aggregate::<i128, Sum, BooleanLt>::try_new(ts_col, predicate, left_col).unwrap();

        let batches = {
            let batch = RecordBatch::try_new(schema.clone(), arrs)?;
            let to_take = vec![4, 9, 9, 1, 1, 1, 3, 3, 3];
            // let to_take = vec![10, 10, 10, 3];
            let mut offset = 0;
            to_take
                .into_iter()
                .map(|v| {
                    let arrs = batch
                        .columns()
                        .iter()
                        .map(|c| c.slice(offset, v).to_owned())
                        .collect::<Vec<_>>();
                    offset += v;
                    arrs
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|v| RecordBatch::try_new(schema.clone(), v).unwrap())
                .collect::<Vec<_>>()
        };

        let spans = vec![1, 2, 3, 4];
        let res = agg.evaluate(&batches, spans, 0).unwrap();
    }
}
