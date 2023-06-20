use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::Int64Array;
use arrow::array::TimestampMillisecondArray;
use arrow::record_batch::RecordBatch;
use arrow2::array::Int128Array;
use arrow2::types::PrimitiveType::Int128;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions::Column;
use datafusion_common::ScalarValue;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::abs_row_id_refs;
use crate::physical_plan::segmentation::accumulator::Accumulator;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use crate::physical_plan::segmentation::boolean_op::Operator;
use crate::physical_plan::segmentation::time_window::TimeWindow;

pub trait Expr {
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<bool>>;
}

#[derive(Debug)]
pub struct Aggregate<T, Op, Acc>
where T: Debug
{
    ts_col: PhysicalExprRef,
    predicate: PhysicalExprRef,
    acc: PhantomData<Acc>,
    op: PhantomData<Op>,
    left_col: PhysicalExprRef,
    right: T,
    time_window: Box<dyn TimeWindow>,
    cur_span: usize,
}

impl<T, Op, Acc> Aggregate<T, Op, Acc>
where
    T: Debug,
    Acc: Accumulator<T>,
    Op: BooleanOp<T>,
{
    pub fn try_new(
        ts_col: Column,
        predicate: PhysicalExprRef,
        left_col: Column,
        right: T,
        time_window: Box<dyn TimeWindow>,
    ) -> Result<Self> {
        let res = Self {
            ts_col: Arc::new(ts_col) as PhysicalExprRef,
            predicate,
            acc: Default::default(),
            op: Default::default(),
            time_window,
            left_col: Arc::new(left_col) as PhysicalExprRef,
            right,
            cur_span: 0,
        };

        Ok(res)
    }
}

// Batch for state
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub ts: TimestampMillisecondArray,
    pub predicate: BooleanArray,
    pub left_values: ArrayRef,
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
        abs_row_id_refs(
            self.row_id,
            self.batches.iter().map(|b| b.batch).collect::<Vec<_>>(),
        )
    }

    // get ts value of current row
    #[inline]
    pub fn ts_value(&self) -> i64 {
        // calculate batch id and row id
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].ts.value(idx)
    }

    #[inline]
    pub fn check_predicate(&self) -> bool {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].predicate.value(idx)
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

    pub fn values(&self) -> &ArrayRef {
        let (batch_id, idx) = self.abs_row_id();
        &self.batches[batch_id].left_values
    }
}

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

impl<Op, Acc> Aggregate<i128, Op, Acc>
where
    Op: BooleanOp<i128>,
    Acc: Accumulator<i128> + Debug,
{
    // calculate expressions
    fn evaluate_batch<'a>(&mut self, batch: &'a RecordBatch) -> Result<Batch<'a>> {
        // timestamp column
        // Optiprism uses millisecond precision
        let ts = self
            .ts_col
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .clone();

        let predicate = self
            .predicate
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();

        let left_values = self.left_col.evaluate(&batch)?.into_array(0);
        // add steps to state
        let res = Batch {
            ts,
            predicate,
            batch,
            left_values,
        };

        Ok(res)
    }

    // take next span if exist
    fn next_span<'a>(&'a mut self, batches: &'a [Batch], spans: &[usize]) -> Option<Span> {
        if self.cur_span == spans.len() {
            return None;
        }

        let span_len = spans[self.cur_span];
        // offset is a sum of prev spans
        let offset = (0..self.cur_span).into_iter().map(|i| spans[i]).sum();
        let rows_count = batches.iter().map(|b| b.len()).sum::<usize>();
        if offset + span_len > rows_count {
            (
                " offset {offset}, span len: {span_len} > rows count: {}",
                rows_count,
            );
            return None;
        }
        self.cur_span += 1;
        Some(Span::new(self.cur_span - 1, offset, span_len, batches))
    }
}
impl<Op, Acc> Expr for Aggregate<i128, Op, Acc>
where
    Op: BooleanOp<i128>,
    Acc: Accumulator<i128> + Debug,
{
    fn evaluate(
        &mut self,
        record_batches: &[RecordBatch],
        spans: Vec<usize>,
        skip: usize,
    ) -> Result<Vec<bool>> {
        // evaluate each batch, e.g. create batch state
        let batches = record_batches
            .iter()
            .map(|b| self.evaluate_batch(b))
            .collect::<Result<Vec<_>>>()?;

        let left_values = batches
            .iter()
            .map(|b| b.left_values.as_any().downcast_ref::<Int64Array>().unwrap())
            .collect::<Vec<_>>();

        let right_value: i128 = self.right.into();
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

        let mut tw = dyn_clone::clone_box(&*self.time_window);
        'main: while let Some(mut span) = self.next_span(&batches, &spans) {
            tw.reset();
            let mut acc: i128 = 0;

            println!("new span");
            let res = loop {
                println!("iteration");
                if !tw.check_bounds(span.ts_value()) {
                    println!("{} out of bounds", span.ts_value());

                    if !span.next_row() {
                        println!("no next row");

                        break true;
                    }
                    continue;
                }
                if !span.check_predicate() {
                    println!("no predicate");
                    if !span.next_row() {
                        println!("no next row");

                        break true;
                    }
                    continue;
                }

                let (batch_id, row_id) = abs_row_id(span.row_id, record_batches);
                acc = Acc::perform(acc, left_values[batch_id].value(row_id) as i128);
                println!("acc after perform {acc}");
                match Op::op() {
                    (Operator::Gt | Operator::GtEq) => {
                        if Op::perform(acc, right_value) {
                            break false;
                        }
                    }
                    _ => {}
                }

                if !span.next_row() {
                    println!("no next rows");
                    break true;
                }
            };

            if !res {
                println!("not pass");
                results.push(false);
                continue;
            }
            println!("pass with acc {acc}");
            let res = match Op::op() {
                Operator::Eq => {
                    println!("{} == {}", acc, right_value);
                    acc == right_value
                }
                Operator::NotEq => acc != right_value,
                Operator::Lt => acc < right_value,
                Operator::LtEq => acc <= right_value,
                Operator::Gt => true,
                Operator::GtEq => true,
                _ => unreachable!("{:?}", Op::op()),
            };

            results.push(res == true);
        }

        Ok(results)
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

    use crate::physical_plan::segmentation::accumulator::Sum;
    // use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::aggregate::{Aggregate, Expr};
    use crate::physical_plan::segmentation::boolean_op::BooleanEq;
    use crate::physical_plan::segmentation::boolean_op::BooleanGt;
    use crate::physical_plan::segmentation::boolean_op::BooleanLt;
    use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::time_window::Between;
    use crate::physical_plan::segmentation::time_window::TimeWindow;
    use crate::physical_plan::PartitionState;

    fn test_batches(batches: Vec<RecordBatch>, mut agg: Box<dyn Expr>) -> bool {
        let pk = vec![
            Arc::new(Column::new_with_schema("user_id", &batches[0].schema()).unwrap())
                as PhysicalExprRef,
        ];
        let mut state = PartitionState::new(pk);
        for batch in batches.clone() {
            match state.push(batch) {
                Ok(Some((batches, spans, skip))) => {
                    let res = agg.evaluate(&batches, spans, skip).unwrap();
                    println!("res: {:?}", res);
                }
                _ => {}
                Err(err) => panic!("{:?}", err),
            }
        }

        match state.finalize() {
            Ok(Some((batches, spans, skip))) => {
                let res = agg.evaluate(&batches, spans, skip).unwrap();
                println!("res: {:?}", res);
            }
            _ => {}
        }

        true
    }
    #[test]
    fn test_sum() {
        let data = r#"
| user_id | ts  | event | v  |
|---------|-----|-------|--------| 
| 0       | 1   | e1    | 1      |
| 0       | 2   | e2    | 2      |
| 0       | 3   | e3    | 3      |
| 0       | 4   | e1    | 4      |

| 0       | 5   | e1    | 1      |
| 0       | 6   | e2    | 1      |
| 0       | 7   | e3    | 1      |
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
            arrow2::datatypes::Field::new("v", arrow2::datatypes::DataType::Int64, true),
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

        let batches = {
            let batch = RecordBatch::try_new(schema.clone(), arrs).unwrap();
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

        // todo add assertions
        {
            println!("sum<100, between 1-20");
            let pk = vec![
                Arc::new(Column::new_with_schema("user_id", &schema).unwrap()) as PhysicalExprRef,
            ];
            let mut state = PartitionState::new(pk);

            let tw = Box::new(Between { from: 1, to: 20 }) as Box<dyn TimeWindow>;
            let mut agg = Aggregate::<i128, BooleanLt, Sum>::try_new(
                ts_col.clone(),
                predicate.clone(),
                left_col.clone(),
                100,
                tw,
            )
            .unwrap();

            test_batches(batches.clone(), Box::new(agg) as Box<dyn Expr>);
        }

        {
            println!("sum>100, between 5-20");
            let pk = vec![
                Arc::new(Column::new_with_schema("user_id", &schema).unwrap()) as PhysicalExprRef,
            ];

            let tw = Box::new(Between { from: 5, to: 30 }) as Box<dyn TimeWindow>;
            let mut agg = Aggregate::<i128, BooleanGt, Sum>::try_new(
                ts_col.clone(),
                predicate.clone(),
                left_col.clone(),
                100,
                tw,
            )
            .unwrap();

            test_batches(batches.clone(), Box::new(agg) as Box<dyn Expr>);
        }

        {
            println!("sum=1, between 5-100");
            let tw = Box::new(Between { from: 5, to: 100 }) as Box<dyn TimeWindow>;
            let mut agg = Aggregate::<i128, BooleanEq, Sum>::try_new(
                ts_col.clone(),
                predicate.clone(),
                left_col.clone(),
                1,
                tw,
            )
            .unwrap();

            test_batches(batches.clone(), Box::new(agg) as Box<dyn Expr>);
        }
    }

    #[test]
    fn it_works() {}
}
