use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::Array;
use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::BooleanBuilder;
use arrow::array::Int64Builder;
use arrow::array::PrimitiveArray;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::filter;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;

use crate::error::Result;
use crate::physical_plan::expressions::segmentation::boolean_op::ComparisonOp;
use crate::physical_plan::expressions::segmentation::boolean_op::Operator;
use crate::physical_plan::expressions::segmentation::check_filter;
use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
use crate::physical_plan::expressions::segmentation::SegmentationExpr;

#[derive(Debug)]
struct CountInner {
    last_hash: u64,
    last_ts: i64,
    out: BooleanBuilder,
    count: i64,
    skip: bool,
}

#[derive(Debug)]
pub struct Count<Op> {
    inner: Arc<Mutex<CountInner>>,
    filter: PhysicalExprRef,
    ts_col: Column,
    time_range: TimeRange,
    op: PhantomData<Op>,
    right: i64,
    time_window: i64,
}

impl<Op> Count<Op> {
    pub fn new(
        filter: PhysicalExprRef,
        ts_col: Column,
        right: i64,
        time_range: TimeRange,
        time_window: Option<i64>,
    ) -> Self {
        let inner = CountInner {
            last_hash: 0,
            last_ts: 0,
            out: BooleanBuilder::with_capacity(10_000),
            count: 0,
            skip: false,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            filter,
            ts_col,
            time_range,
            op: Default::default(),
            right,
            time_window: time_window
                .map(|t| t)
                .unwrap_or(Duration::days(365).num_milliseconds()),
        }
    }
}

impl<Op> SegmentationExpr for Count<Op>
where Op: ComparisonOp<i64>
{
    fn evaluate(&self, record_batch: &RecordBatch, hashes: &[u64]) -> Result<Option<BooleanArray>> {
        // println!(".");
        // println!("{:?}", record_batch.columns()[0]);
        let ts = self
            .ts_col
            .evaluate(record_batch)?
            .into_array(record_batch.num_rows())
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .clone();

        let to_filter = self
            .filter
            .evaluate(record_batch)?
            .into_array(record_batch.num_rows())
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .clone();
        let mut inner = self.inner.lock().unwrap();
        for (idx, hash) in hashes.iter().enumerate() {
            if inner.last_hash == 0 {
                inner.last_hash = *hash;
                inner.last_ts = ts.value(idx);
            }
            if check_filter(&to_filter, idx) == false {
                // println!("skip1");
                continue;
            }
            if !self.time_range.check_bounds(ts.value(idx)) {
                // println!("skip2");
                continue;
            }
            if *hash != inner.last_hash {
                // println!("{} != {}", *hash, inner.last_hash);
                let count = inner.count;
                let last_hash = inner.last_hash;
                inner.last_hash = *hash;
                inner.count = 0;
                inner.last_ts = ts.value(idx);
                let skip = inner.skip;
                inner.skip = false;
                if !skip {
                    // println!("{} {}", count, self.right);
                    let res = match Op::op() {
                        Operator::Lt => count < self.right,
                        Operator::LtEq => count <= self.right,
                        Operator::Eq => count == self.right,
                        Operator::NotEq => count != self.right,
                        Operator::Gt => count > self.right,
                        Operator::GtEq => count >= self.right,
                    };
                    if !res {
                        // println!("a1 false {}", last_hash);
                        inner.out.append_value(false);
                        inner.count += 1;
                        continue;
                    }
                    // println!("a2 true {}", last_hash);
                    inner.out.append_value(true);
                }
            } else if !inner.skip && ts.value(idx) - inner.last_ts >= self.time_window {
                let count = inner.count;
                inner.count = 0;
                let res = match Op::op() {
                    Operator::Lt => count < self.right,
                    Operator::LtEq => count <= self.right,
                    Operator::Eq => count == self.right,
                    Operator::NotEq => count != self.right,
                    Operator::Gt => count > self.right,
                    Operator::GtEq => count >= self.right,
                };
                if !res {
                    // println!("a3 false {}", *hash);
                    inner.out.append_value(false);
                    inner.skip = true;
                } else {
                    inner.last_ts = ts.value(idx);
                }
            } else if inner.skip {
                continue;
            }
            inner.count += 1;
        }

        if inner.out.len() > 0 {
            Ok(Some(inner.out.finish()))
        } else {
            Ok(None)
        }
    }

    fn finalize(&self) -> Result<BooleanArray> {
        let mut inner = self.inner.lock().unwrap();
        if inner.skip {
            // println!("a4 false {}", inner.last_hash);
            inner.out.append_value(false);
            return Ok(inner.out.finish());
        }

        let count = inner.count;
        let res = match Op::op() {
            Operator::Lt => count < self.right,
            Operator::LtEq => count <= self.right,
            Operator::Eq => count == self.right,
            Operator::NotEq => count != self.right,
            Operator::Gt => count > self.right,
            Operator::GtEq => count >= self.right,
        };

        if !res {
            // println!("a5 false {}", inner.last_hash);
            inner.out.append_value(false);
        } else {
            // println!("a6 true {}", inner.last_hash);
            inner.out.append_value(true);
        }
        Ok(inner.out.finish())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Array;
    use arrow::array::ArrayRef;
    use arrow::array::BooleanArray;
    use arrow::array::Int64Array;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::SchemaRef;
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::hash_utils::create_hashes;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion_common::ScalarValue;
    use datafusion_expr::binary_expr;
    use datafusion_expr::lit;
    use datafusion_expr::Expr;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_table_v1;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::segmentation::boolean_op;
    use crate::physical_plan::expressions::segmentation::boolean_op::Gt;
    use crate::physical_plan::expressions::segmentation::count::Count;
    use crate::physical_plan::expressions::segmentation::time_range::TimeRange;
    use crate::physical_plan::expressions::segmentation::SegmentationExpr;

    #[test]
    fn test_predicate() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 0            | 1      | e1          |
| 0            | 2      | e2          |
| 0            | 3      | e3          |
| 0            | 4      | e1          |
| 0            | 5      | e1          |
| 0            | 6      | e2          |
| 0            | 7      | e3          |

| 1            | 8      | e1          |
| 1            | 9      | e3          |
| 1            | 10      | e1          |
| 1            | 11      | e2          |

| 2            | 12      | e2          |
| 2            | 13      | e1          |
| 2            | 14      | e2          |
"#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();

        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                2,
                TimeRange::None,
                None,
            );
            let res = count.evaluate(&res, &hash_buf).unwrap();
            let right = BooleanArray::from(vec![true, false]);
            assert_eq!(res, Some(right));

            let res = count.finalize().unwrap();
            let right = BooleanArray::from(vec![false]);
            assert_eq!(res, right);
        }
        {
            let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
            let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e2".to_string()))));
            let f = BinaryExpr::new(left, Operator::Eq, right);
            let mut count = Count::<Gt>::new(
                Arc::new(f) as PhysicalExprRef,
                Column::new_with_schema("ts", &res.schema()).unwrap(),
                1,
                TimeRange::None,
                None,
            );
            let res = count.evaluate(&res, &hash_buf).unwrap();
            let right = BooleanArray::from(vec![true, false]);
            assert_eq!(res, Some(right));

            let res = count.finalize().unwrap();
            let right = BooleanArray::from(vec![true]);
            assert_eq!(res, right);
        }
    }

    #[test]
    fn time_range() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) |
    |--------------|--------|-------------|
    | 0            | 1      | e1          |
    | 0            | 2      | e2          |
    | 0            | 3      | e3          |
    | 0            | 4      | e1          |
    | 0            | 5      | e1          |
    | 0            | 6      | e2          |
    | 0            | 7      | e3          |
    | 1            | 5      | e1          |
    | 1            | 6      | e3          |
    | 1            | 7      | e1          |
    | 1            | 8      | e2          |
    | 2            | 9      | e1          |
    "#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();
        let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            1,
            TimeRange::From(2),
            None,
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = BooleanArray::from(vec![true, true]);
        assert_eq!(res, Some(right));
    }

    #[test]
    fn time_window_success() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) |
    |--------------|--------|-------------|
    | 0            | 0     | e1          |
    | 0            | 1     | e1          |

    | 0            | 2     | e1          |
    | 0            | 3     | e1          |

    | 1            | 11     | e1          |
    | 1            | 12     | e1          |

    | 1            | 13     | e1          |
    | 1            | 14     | e1          |

    | 2            | 16     | e1          |
    | 2            | 17     | e1          |

    | 2            | 18     | e1          |

    | 3            | 19     | e1          |
    | 3            | 20     | e1          |

    | 3            | 22     | e1          |
    | 3            | 23     | e1          |

   | 3            | 24     | e1          |
   | 3            | 24     | e1          |

    "#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();
        let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            1,
            TimeRange::None,
            Some(Duration::nanoseconds(2).num_nanoseconds().unwrap()),
        );
        let res = count.evaluate(&res, &hash_buf).unwrap();
        let right = BooleanArray::from(vec![true, true, false]);
        assert_eq!(res, Some(right));

        let res = count.finalize().unwrap();
        let right = BooleanArray::from(vec![true]);
        assert_eq!(res, right);
    }

    #[test]
    fn time_window_finalize_fail() {
        let data = r#"
    | user_id(i64) | ts(ts) | event(utf8) |
    |--------------|--------|-------------|
    | 0            | 0     | e1          |
    | 0            | 1     | e1          |

    | 0            | 2     | e1          |

    "#;

        let res = parse_markdown_table_v1(data).unwrap();

        let mut random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
        let mut hash_buf = vec![];
        hash_buf.resize(res.num_rows(), 0);
        create_hashes(
            &vec![res.columns()[0].clone()],
            &mut random_state,
            &mut hash_buf,
        )
        .unwrap();
        let left = Arc::new(Column::new_with_schema("event", &res.schema()).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("e1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<Gt>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &res.schema()).unwrap(),
            1,
            TimeRange::None,
            Some(Duration::nanoseconds(2).num_nanoseconds().unwrap()),
        );

        let res = count.evaluate(&res, &hash_buf).unwrap();
        assert_eq!(res, None);

        let res = count.finalize().unwrap();
        let right = BooleanArray::from(vec![false]);
        assert_eq!(res, right);
    }

    #[test]
    fn test_2() {
        let data = r#"
| user_id(i64) | ts(ts) | event(utf8) |
|--------------|--------|-------------|
| 1            | 1      | 1           |
|              |        |             |
| 2            | 2      | 1           |
|              |        |             |
| 3            | 3      | 1           |
| 3            | 4      | 1           |
| 3            | 5      | 1           |
|              |        |             |
| 3            | 6      | 1           |
| 3            | 7      | 1           |
| 3            | 8      | 1           |
|              |        |             |
| 3            | 9      | 1           |
| 3            | 10     | 1           |
| 3            | 11     | 1           |
|              |        |             |
| 3            | 12     | 1           |
| 4            | 13     | 1           |
| 5            | 14     | 1           |
| 6            | 15     | 1           |
| 6            | 16     | 1           |
| 6            | 17     | 1           |
| 7            | 18     | 1           |
| 7            | 19     | 1           |
| 8            | 20     | 1           |
|              |        |             |
| 9            | 21     | 1           |
| 9            | 22     | 1           |
| 10           | 23     | 1           |
| 11           | 24     | 1           |
|              |        |             |
| 12           | 25     | 1           |
| 12           | 26     | 1           |
| 12           | 27     | 1           |
| 12           | 28     | 1           |
|              |        |             |
| 12           | 29     | 1           |
| 12           | 30     | 1           |
| 12           | 31     | 1           |
| 12           | 32     | 1           |
|              |        |             |
| 12           | 33     | 1           |
| 12           | 34     | 1           |
|              |        |             |
| 12           | 35     | 1           |
| 13           | 36     | 1           |
| 14           | 37     | 1           |
| 15           | 38     | 1           |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let left = Arc::new(Column::new_with_schema("event", &schema).unwrap());
        let right = Arc::new(Literal::new(ScalarValue::Utf8(Some("1".to_string()))));
        let f = BinaryExpr::new(left, Operator::Eq, right);
        let mut count = Count::<boolean_op::Eq>::new(
            Arc::new(f) as PhysicalExprRef,
            Column::new_with_schema("ts", &schema).unwrap(),
            1,
            TimeRange::None,
            Some(Duration::nanoseconds(2).num_nanoseconds().unwrap()),
        );

        for batch in batches {
            let mut hash_buf = batch.columns()[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap() as u64)
                .collect::<Vec<_>>();

            let res = count.evaluate(&batch, &hash_buf).unwrap();
            // println!("{:?}", res);
        }

        let res = count.finalize().unwrap();
        println!("{:?}", res);
    }
}
