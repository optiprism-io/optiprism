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
use crate::physical_plan::segmentation::aggregate_function::AggregateFunction;
use crate::physical_plan::segmentation::aggregate_function::Primitive;
use crate::physical_plan::segmentation::boolean_op::BooleanOp;
use crate::physical_plan::segmentation::boolean_op::Operator;
use crate::physical_plan::segmentation::time_range::TimeRange;
use crate::physical_plan::segmentation::Expr;
use crate::span;

span!(Batch);
#[derive(Debug)]
pub struct CountPredicate<Op> {
    ts_col: PhysicalExprRef,
    predicate: PhysicalExprRef,
    op: PhantomData<Op>,
    right: i64,
    time_range: TimeRange,
    time_window: i64,
    cur_span: usize,
}

impl<Op> CountPredicate<Op>
where Op: BooleanOp<i64>
{
    pub fn try_new(
        ts_col: Column,
        predicate: PhysicalExprRef,
        right: i64,
        time_range: TimeRange,
        time_window: Option<Duration>,
    ) -> Result<Self> {
        let res = Self {
            ts_col: Arc::new(ts_col) as PhysicalExprRef,
            predicate,
            op: Default::default(),
            time_range,
            right,
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
    pub predicate: BooleanArray,
    pub batch: &'a RecordBatch,
}

impl<'a> Batch<'a> {
    pub fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

impl<Op> CountPredicate<Op>
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

        let predicate = self
            .predicate
            .evaluate(&batch)?
            .into_array(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();

        // add steps to state
        let res = Batch {
            ts,
            predicate,
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

macro_rules! aggregate_column_int {
    ($acc_ty:ty,$array_type:ident) => {
        impl<Op> Expr for AggregateColumn<$acc_ty, $array_type, Op>
        where Op: BooleanOp<$acc_ty>
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
                    .map(|b| {
                        b.left_values
                            .as_any()
                            .downcast_ref::<$array_type>()
                            .unwrap()
                    })
                    .collect::<Vec<_>>();

                let right_value: $acc_ty = self.right.into();
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
                let mut agg = self.aggregator.clone();
                let time_window = self.time_window;
                'main: while let Some(mut span) = self.next_span(&batches, &spans) {
                    let cur_time = span.ts_value();
                    let mut max_time = cur_time + time_window;
                    let res = 'time_window: loop {
                        agg.reset();
                        let res = loop {
                            if span.ts_value() >= max_time {
                                break true;
                            }

                            if !time_range.check_bounds(span.ts_value()) {
                                if !span.next_row() {
                                    break 'time_window true;
                                }
                                continue;
                            }
                            if !span.check_predicate() {
                                if !span.next_row() {
                                    break 'time_window true;
                                }
                                continue;
                            }

                            let (batch_id, row_id) = abs_row_id(span.row_id, record_batches);
                            let agg_res =
                                agg.accumulate(left_values[batch_id].value(row_id) as $acc_ty);
                            match Op::op() {
                                (Operator::Gt | Operator::GtEq) => {
                                    if Op::perform(agg_res, right_value) {
                                        break 'time_window false;
                                    }
                                }
                                _ => {}
                            }

                            if !span.next_row() {
                                break 'time_window true;
                            }
                        };

                        if !res {
                            break false;
                        }
                        let acc = agg.result();

                        let res = match Op::op() {
                            Operator::Eq => acc == right_value,
                            Operator::NotEq => acc != right_value,
                            Operator::Lt => acc < right_value,
                            Operator::LtEq => acc <= right_value,
                            Operator::Gt => true,
                            Operator::GtEq => true,
                            _ => unreachable!("{:?}", Op::op()),
                        };

                        if !res {
                            break 'time_window false;
                        }
                        max_time += time_window
                    };

                    results.push(res == true);
                }

                Ok(results)
            }
        }
    };
}

impl<Op> Expr for CountPredicate<Op>
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
        let mut count = 0;
        let mut right = self.right;
        let time_window = self.time_window;
        'main: while let Some(mut span) = self.next_span(&batches, &spans) {
            let cur_time = span.ts_value();
            let mut max_time = cur_time + time_window;
            let res = loop {
                count = 0;
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
                    if !span.check_predicate() {
                        if !span.next_row() {
                            break;
                        }
                        continue;
                    }

                    count += 1;
                    // perf: we can optimize Gt and GtEq by checking early

                    if !span.next_row() {
                        break;
                    }
                }

                let res = match Op::op() {
                    Operator::Lt => count < right,
                    Operator::LtEq => count <= right,
                    Operator::NotEq => count != right,
                    Operator::Eq => count == right,
                    Operator::Gt => count > right,
                    Operator::GtEq => count >= right,
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

    use crate::physical_plan::segmentation::aggregate_function::AggregateFunction;
    use crate::physical_plan::segmentation::boolean_op::BooleanEq;
    use crate::physical_plan::segmentation::boolean_op::BooleanGt;
    use crate::physical_plan::segmentation::boolean_op::BooleanLt;
    use crate::physical_plan::segmentation::boolean_op::Operator;
    // use crate::physical_plan::segmentation::boolean_op::Operator;
    use crate::physical_plan::segmentation::count_predicate::CountPredicate;
    use crate::physical_plan::segmentation::time_range;
    use crate::physical_plan::segmentation::time_range::from_milli;
    use crate::physical_plan::segmentation::time_range::TimeRange;
    use crate::physical_plan::segmentation::Expr;
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
| user_id | ts  | event |
|---------|-----|-------|
| 0       | 1   | e1    |
| 0       | 2   | e1    |
| 0       | 3   | e1    |
| 0       | 4   | e1    |
| 0       | 5   | e1    |
| 0       | 6   | e1    |
| 0       | 7   | e1    |

| 1       | 5   | e1    |
| 1       | 6   | e1    |
| 1       | 7   | e1    |
| 1       | 8   | e1    |

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

        let predicate = {
            let l = Column::new_with_schema("event", &schema).unwrap();
            let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
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

            let mut agg = CountPredicate::<BooleanEq>::try_new(
                ts_col.clone(),
                predicate.clone(),
                2,
                TimeRange::new_between_milli(1, 100),
                Some(Duration::milliseconds(2)),
            )
            .unwrap();

            let batches = vec![batch];
            test_batches(batches.clone(), Box::new(agg) as Box<dyn Expr>);
        }
    }
}
