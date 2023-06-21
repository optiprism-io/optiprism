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
use chrono::DateTime;
use chrono::Duration;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::expressions::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use num::Integer;
use num_traits::Bounded;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::PrimInt;

use crate::error::Result;
use crate::physical_plan::abs_row_id;
use crate::physical_plan::abs_row_id_refs;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use crate::physical_plan::segmentation::boolean_op::Operator;
use crate::physical_plan::segmentation::time_range::TimeRange;
use crate::physical_plan::segmentation::SegmentationExpr;
use crate::span;

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

    pub fn reset(&mut self) {
        self.row_id = 0;
    }

    #[inline]
    pub fn abs_row_id(&self) -> (usize, usize) {
        abs_row_id_refs(
            self.row_id + self.offset,
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
    pub fn check_left_predicate(&self) -> bool {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].left_predicate.value(idx)
    }

    #[inline]
    pub fn check_right_predicate(&self) -> bool {
        let (batch_id, idx) = self.abs_row_id();
        self.batches[batch_id].right_predicate.value(idx)
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

    #[inline]
    pub fn is_next_row(&self) -> bool {
        if self.row_id >= self.len - 1 {
            return false;
        }

        true
    }
}

#[derive(Debug)]
pub struct CountPredicateRelative<Op> {
    ts_col: PhysicalExprRef,
    left_predicate: PhysicalExprRef,
    op: PhantomData<Op>,
    right_predicate: PhysicalExprRef,
    time_range: TimeRange,
    time_window: i64,
    cur_span: usize,
}

impl<Op> CountPredicateRelative<Op>
where Op: BooleanOp<i64>
{
    pub fn try_new(
        ts_col: Column,
        left_predicate: PhysicalExprRef,
        right_predicate: PhysicalExprRef,
        time_range: TimeRange,
        time_window: Option<Duration>,
    ) -> Result<Self> {
        let res = Self {
            ts_col: Arc::new(ts_col) as PhysicalExprRef,
            left_predicate,
            op: Default::default(),
            right_predicate,
            time_range,
            cur_span: 0,
            time_window: time_window
                .map(|v| v.num_milliseconds())
                .or(Some(Duration::days(365 * 10).num_milliseconds()))
                .unwrap(),
        };

        Ok(res)
    }
}

// Batch for state
#[derive(Debug, Clone)]
pub struct Batch<'a> {
    pub ts: TimestampMillisecondArray,
    pub left_predicate: BooleanArray,
    pub right_predicate: BooleanArray,
    pub batch: &'a RecordBatch,
}

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

impl<Op> CountPredicateRelative<Op>
where Op: BooleanOp<i64>
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

        let left_predicate = self
            .left_predicate
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let right_predicate = self
            .right_predicate
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        // add steps to state
        let res = Batch {
            ts,
            left_predicate,
            right_predicate,
            batch,
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

impl<Op> SegmentationExpr for CountPredicateRelative<Op>
where Op: BooleanOp<i64>
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
        let mut time_range = self.time_range.clone();
        let mut left_count = 0;
        let mut right_count = 0;
        let time_window = self.time_window;
        'main: while let Some(mut span) = self.next_span(&batches, &spans) {
            let cur_time = span.ts_value();
            let mut max_time = cur_time + time_window;
            let res = loop {
                left_count = 0;
                right_count = 0;
                loop {
                    if span.ts_value() >= max_time {
                        break;
                    }

                    if !time_range.check_bounds(span.ts_value()) {
                        if !span.next_row() {
                            break;
                        }
                        continue;
                    }
                    if !span.check_left_predicate() {
                        if !span.next_row() {
                            break;
                        }
                        continue;
                    }

                    left_count += 1;
                    // perf: we can optimize Gt and GtEq by checking early

                    if !span.next_row() {
                        break;
                    }
                }

                span.reset();

                loop {
                    if span.ts_value() >= max_time {
                        break;
                    }

                    if !time_range.check_bounds(span.ts_value()) {
                        if !span.next_row() {
                            break;
                        }
                        continue;
                    }
                    if !span.check_right_predicate() {
                        if !span.next_row() {
                            break;
                        }
                        continue;
                    }

                    right_count += 1;
                    // perf: we can optimize Gt and GtEq by checking early

                    if !span.next_row() {
                        break;
                    }
                }

                let res = match Op::op() {
                    Operator::Lt => left_count < right_count,
                    Operator::LtEq => left_count <= right_count,
                    Operator::NotEq => left_count != right_count,
                    Operator::Eq => left_count == right_count,
                    Operator::Gt => left_count > right_count,
                    Operator::GtEq => left_count >= right_count,
                    _ => unreachable!(),
                };

                if !res {
                    break false;
                }

                if !span.is_next_row() {
                    break true;
                }

                max_time += time_window
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
    use arrow2::compute::arithmetics::time;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::NaiveTime;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;

    use crate::physical_plan::segmentation::boolean_op::BooleanEq;
    use crate::physical_plan::segmentation::boolean_op::BooleanGt;
    use crate::physical_plan::segmentation::boolean_op::BooleanLt;
    use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::count_predicate_relative::CountPredicateRelative;
    // use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::time_range;
    use crate::physical_plan::segmentation::time_range::from_milli;
    use crate::physical_plan::segmentation::time_range::TimeRange;
    use crate::physical_plan::segmentation::SegmentationExpr;
    use crate::physical_plan::PartitionState;

    fn test_batches(batches: Vec<RecordBatch>, mut agg: Box<dyn SegmentationExpr>) -> bool {
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
| user_id | ts  | event |
|---------|-----|-------|
| 0       | 1   | e1    |
| 0       | 2   | e2    |
| 0       | 3   | e1    |
| 0       | 4   | e2    |
| 0       | 5   | e1    |
| 0       | 6   | e2    |
| 0       | 7   | e1    |
| 0       | 8   | e2    |

| 1       | 5   | e1    |
| 1       | 6   | e1    |
| 1       | 7   | e2    |
| 1       | 8   | e2    |

| 2       | 9   | e1    |
| 2       | 10  | e1    |
| 2       | 11  | e1    |
| 2       | 12  | e1    |
| 2       | 13  | e1    |
| 2       | 14  | e1    |

| 3       | 15  | e1    |
| 3       | 16  | e1    |
| 3       | 17  | e1    |

| 4       | 18  | e1    |
| 4       | 19  | e1    |
| 4       | 20  | e1    |

| 5       | 21  | e1    |
| 5       | 22  | e1    |

| 6       | 23  | e1    |

| 7       | 24  | e1    |

| 8       | 25  | e1    |

| 9       | 26  | e1    |
| 9       | 27  | e1    |
| 9       | 28  | e1    |

| 10      | 29  | e1    |
| 10      | 30  | e1    |
| 10      | 31  | e1    |
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

        let left_predicate = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), datafusion_expr::Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let right_predicate = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e2".to_string())));
            let expr = BinaryExpr::new(Arc::new(l), datafusion_expr::Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let ts_col = Column::new_with_schema("ts", &schema).unwrap();

        let batch = RecordBatch::try_new(schema.clone(), arrs).unwrap();

        // todo add assertions
        {
            println!("count>2, window 100ms");
            let pk = vec![
                Arc::new(Column::new_with_schema("user_id", &schema).unwrap()) as PhysicalExprRef,
            ];
            let mut state = PartitionState::new(pk);

            let mut agg = CountPredicateRelative::<BooleanEq>::try_new(
                ts_col.clone(),
                left_predicate.clone(),
                right_predicate.clone(),
                TimeRange::new_between_milli(1, 100),
                None,
            )
            .unwrap();

            let batches = vec![batch];
            test_batches(batches.clone(), Box::new(agg) as Box<dyn SegmentationExpr>);
        }
    }
}
