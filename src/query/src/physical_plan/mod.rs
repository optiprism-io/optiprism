use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::hash_utils::create_hashes;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::Result as DFResult;

mod dictionary_decode;
pub mod expressions;
pub mod merge;
mod pivot;
pub mod planner;
mod unpivot;
mod segmentation;
mod funnel;
// pub mod merge;
// pub mod planner;

struct PartitionState {
    random_state: ahash::RandomState,
    hash_buffer: Vec<u64>,
    buf: Vec<RecordBatch>,
    last_value: Option<u64>,
    last_span: usize,
    spans: Vec<usize>,
    c: usize,
    partition_key: Vec<Arc<dyn PhysicalExpr>>,
}

use std::sync::Mutex;
use lazy_static::lazy_static;
use std::collections::VecDeque;

lazy_static! {
    static ref ARRAY: Mutex<VecDeque<usize>> = Mutex::new(VecDeque::from(vec![
            0,
            4,
            7,
            0,
            0,
            0,
        ]));
}

impl PartitionState {
    pub fn new(partition_key: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            random_state: ahash::RandomState::with_seeds(0, 0, 0, 0),
            hash_buffer: vec![],
            buf: Vec::with_capacity(10),
            last_value: None,
            last_span: 0,
            spans: vec![0],
            c: 0,
            partition_key,
        }
    }

    pub fn push(&mut self, batch: RecordBatch) -> DFResult<Option<(Vec<RecordBatch>, Vec<usize>, usize)>> {
        self.buf.push(batch.clone());
        let arrays = self.partition_key
            .iter()
            .map(|expr| {
                Ok(expr.evaluate(&batch)?.into_array(batch.num_rows()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let num_rows = batch.num_rows();

        self.hash_buffer.clear();
        self.hash_buffer.resize(num_rows, 0);
        create_hashes(&arrays, &mut self.random_state, &mut self.hash_buffer)?;

        let mut take = false;
        for (idx, v) in self.hash_buffer.iter().enumerate() {
            if self.last_value.is_none() {
                self.last_value = Some(*v);
            }

            if self.last_value != Some(*v) {
                self.spans.push(0);
                take = true;
            }
            let i = self.spans.len() - 1;
            self.spans[i] += 1;
            self.last_value = Some(*v);
        };

        if self.buf.len() > 1 && take {
            let mut take_batches = self.buf.drain(..self.buf.len() - 1).collect::<Vec<_>>();
            take_batches.push(self.buf.last().unwrap().to_owned());
            let take_spans = self.spans.drain(..self.spans.len() - 1).collect::<Vec<usize>>();

            // println!("buf {:?}", take_batches.iter().map(|v| v.columns()[0].clone()).collect::<Vec<_>>());
            // println!("spans {:?}", take_spans);
            let mut offset = ARRAY.lock().unwrap();
            let o = offset.drain(..1).collect::<Vec<_>>()[0];
            return Ok(Some((take_batches, take_spans, o)));
        }
        Ok(None)
    }

    pub fn finalize(&mut self) -> DFResult<Option<(Vec<RecordBatch>, Vec<usize>, usize)>> {
        if self.spans.len() > 0 {
            let batches = self.buf.drain(0..).collect();
            let spans = self.spans.drain(0..).collect();

            let mut offset = ARRAY.lock().unwrap();
            let o = offset.drain(..1).collect::<Vec<_>>()[0];
            return Ok(Some((batches, spans, o)));
        }

        Ok(None)
    }
}

#[inline]
pub fn abs_row_id(row_id: usize, batches: &[RecordBatch]) -> (usize, usize) {
    let mut batch_id = 0;
    let mut idx = row_id;
    for batch in batches.iter() {
        if idx < batch.num_rows() {
            break;
        }
        idx -= batch.num_rows();
        batch_id += 1;
    }
    (batch_id, idx)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use arrow::array::{Array, ArrayRef, BooleanBuilder, Int32Array, Int64Array, TimestampMillisecondBuilder, UInt32Array, UInt32Builder};
    use arrow::compute::take;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::error::ArrowError;
    use arrow::ipc::BoolBuilder;
    use arrow::record_batch::RecordBatch;
    use chrono::Duration;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{expressions, PhysicalExprRef};
    use store::arrow_conversion::arrow2_to_arrow1;
    use store::test_util::parse_markdown_table;
    use crate::event_eq;
    use crate::physical_plan::expressions::funnel::{Count, FunnelExpr, FunnelResult, Options, Touch};
    use crate::physical_plan::expressions::funnel::StepOrder::*;
    use crate::physical_plan::expressions::funnel::test_utils::event_eq_;
    // use crate::physical_plan::expressions::funnel::{Count, FunnelExpr, Options, Touch};
    use crate::physical_plan::{abs_row_id, PartitionState};

    #[test]
    fn test_batches_state() -> anyhow::Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]);

        let batches = {
            let v = vec![
                vec![0, 0, 0, 0],
                vec![1, 1, 1, 1, 2, 2, 2, 2, 2],
                vec![2, 3, 3, 3, 4, 4, 4, 5, 5],
                vec![6],
                vec![6],
                vec![6],
                vec![7, 7, 7],
                vec![8, 8, 8],
            ];


            v.into_iter()
                .map(|v| {
                    let arrays = vec![
                        Arc::new(Int64Array::from(v)) as ArrayRef,
                    ];
                    RecordBatch::try_new(Arc::new(schema.clone()), arrays.clone()).unwrap()
                })
                .collect::<Vec<_>>()
        };


        let col = Arc::new(Column::new_with_schema("a", &schema)?) as PhysicalExprRef;
        let mut state = PartitionState::new(vec![col]);

        let mut batches_res = vec![];
        let mut spans_res = vec![];
        let mut skip_res = vec![];
        for (idx, batch) in batches.into_iter().enumerate() {
            let res = state.push(batch)?;
            match res {
                None => {}
                Some((batches, spans, skip)) => {
                    batches_res.push(batches);
                    spans_res.push(spans);
                    skip_res.push(skip);
                }
            }
        }

        let res = state.finalize()?;
        match res {
            None => println!("none"),
            Some((batches, spans, skip)) => {
                batches_res.push(batches);
                spans_res.push(spans);
                skip_res.push(skip);
            }
        }

        let exp_batches = vec![
            vec![ //i1
                  vec![
                      0, 0, 0, 0, // 4
                  ],
                  vec![
                      1, 1, 1, 1, // 4
                      2, 2, 2, 2, 2,
                  ],
            ],
            vec![ //i2
                  vec![
                      1, 1, 1, 1, // skip 4
                      2, 2, 2, 2, 2, // 6
                  ],
                  vec![
                      2,
                      3, 3, 3, // 3
                      4, 4, 4, // 3
                      5, 5,
                  ],
            ],
            vec![ //i3
                  vec![
                      2,
                      3, 3, 3,
                      4, 4, 4, // skip 7
                      5, 5, // 2
                  ],
                  vec![6],
            ],
            vec![ //i4
                  vec![6],
                  vec![6],
                  vec![6], // 3
                  vec![7, 7, 7],
            ],
            vec![ //i5
                  vec![7, 7, 7], // 3
                  vec![8, 8, 8],
            ],
            vec![ //i6
                  vec![8, 8, 8], // 3
            ],
        ];

        for (res_id, batches) in batches_res.iter().enumerate() {
            for (batch_id, vals) in batches.into_iter().enumerate() {
                let l = vals.columns()[0].as_any().downcast_ref::<Int64Array>().unwrap().clone();
                let r = Int64Array::from(exp_batches[res_id][batch_id].clone());
                assert_eq!(l, r);
            }
        }
        let exp_spans = vec![
            vec![4, 4],
            vec![6, 3, 3],
            vec![2],
            vec![3],
            vec![3],
            vec![3],
        ];
        assert_eq!(spans_res, exp_spans);

        let exp_skips = vec![
            0,
            4,
            7,
            0,
            0,
            0,
        ];
        assert_eq!(skip_res, exp_skips);
        Ok(())
    }

    #[test]
    fn test_funnel() {
        let fields = vec![
            Field::new("user_id", DataType::UInt64, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("event", DataType::Utf8, true),
            Field::new("const", DataType::Int64, true),
        ];
        let schema = Arc::new(Schema::new(fields.clone())) as SchemaRef;

        let pre_batches = vec![
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 0       | 0  | e1    | 1     |
| 0       | 1  | e2    | 1     |
| 0       | 2  | e3    | 1     |
| 0       | 3  | e3    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 1       | 4  | e1    | 1     |
| 1       | 5  | e2    | 1     |
| 1       | 6  | e3    | 1     |
| 1       | 6  | e3    | 1     |
| 2       | 7  | e2    | 1     |
| 2       | 8  | e2    | 1     |
| 2       | 10  | e2    | 1     |
| 2       | 11  | e2    | 1     |
| 2       | 12  | e1    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 2       | 13  | e2    | 1     |
| 3       | 14  | e2    | 1     |
| 3       | 15  | e1    | 1     |
| 3       | 16  | e2    | 1     |
| 4       | 17  | e1    | 1     |
| 4       | 18  | e2    | 1     |
| 4       | 19  | e3    | 1     |
| 5       | 20  | e1    | 1     |
| 5       | 21  | e2    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 6       | 22  | e1    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 6       | 23  | e2    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 6       | 24  | e3    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 7       | 25  | e1    | 1     |
| 7       | 26  | e2    | 1     |
| 7       | 27  | e3    | 1     |
"#,
            r#"
| user_id | ts | event | const |
|---------|----|-------|-------|
| 8       | 28  | e1    | 1     |
| 8       | 29  | e2    | 1     |
| 8       | 30  | e3    | 1     |
"#,
        ];

        let fields2 = vec![
            arrow2::datatypes::Field::new("user_id", arrow2::datatypes::DataType::UInt64, false),
            arrow2::datatypes::Field::new("ts", arrow2::datatypes::DataType::Timestamp(arrow2::datatypes::TimeUnit::Millisecond, None), false),
            arrow2::datatypes::Field::new("event", arrow2::datatypes::DataType::Utf8, true),
            arrow2::datatypes::Field::new("const", arrow2::datatypes::DataType::Int64, true),
        ];

        let batches = pre_batches.into_iter().map(|pb| {
            let res = parse_markdown_table(pb, &fields2).unwrap();
            let (arrs, fields) = res.
                into_iter().
                zip(fields2.clone()).
                map(|(arr, field)| arrow2_to_arrow1(arr, field).unwrap()).
                unzip();

            let schema = Arc::new(Schema::new(fields)) as SchemaRef;

            RecordBatch::try_new(schema, arrs).unwrap()
        }).collect::<Vec<_>>();

        let out_schema = Schema::new(
            vec![
                Field::new("is_completed", DataType::Boolean, false),
                Field::new("converted_steps", DataType::UInt32, false),
                Field::new("user_id", DataType::UInt64, false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
                Field::new("event", DataType::Utf8, true),
                Field::new("const", DataType::Int64, true),
            ]);

        let out_schema = Arc::new(out_schema) as SchemaRef;
        let col = Arc::new(Column::new_with_schema("user_id", &schema).unwrap()) as PhysicalExprRef;
        let mut state = PartitionState::new(vec![col]);


        let opts = Options {
            ts_col: Column::new_with_schema("ts", schema.as_ref()).unwrap(),
            window: Duration::seconds(15),
            steps: event_eq!(schema, "e1" Sequential, "e2" Sequential),

            exclude: None,
            constants: None,
            count: Count::Unique,
            filter: None,
            touch: Touch::First,
        };
        let mut funnel = FunnelExpr::new(opts);

        for batch in batches {
            if let Some((batches, spans, skip)) = state.push(batch).unwrap() {
                let mut is_completed_col: HashMap<usize, BooleanBuilder> = HashMap::new();
                let mut converted_steps_col: HashMap<usize, UInt32Builder> = HashMap::new();
                let mut to_take: HashMap<usize, Vec<usize>> = HashMap::new();

                let res = funnel.evaluate(&batches, spans.clone(), skip).unwrap();
                println!("{:?}", res);

                let mut offset = skip;
                for (span, fr) in spans.into_iter().zip(res.into_iter()) {
                    let (batch_id, row_id) = abs_row_id(offset, &batches);
                    to_take.entry(batch_id).or_default().push(row_id);
                    match fr {
                        FunnelResult::Completed(steps) => {
                            is_completed_col.entry(batch_id).or_default().append_value(true);
                            converted_steps_col.entry(batch_id).or_default().append_value(steps.len() as u32)
                        }
                        FunnelResult::Incomplete(steps, stepn) => {
                            is_completed_col.entry(batch_id).or_default().append_value(false);
                            converted_steps_col.entry(batch_id).or_default().append_value(stepn as u32);
                        }
                    }
                    offset += span;
                }

                let batches = to_take.into_iter().map(|(batch_id, rows)| {
                    let take_arr = rows
                        .iter()
                        .map(|b| Some(*b as u32))
                        .collect::<Vec<_>>();
                    let take_arr = UInt32Array::from(take_arr);
                    let cols = batches[batch_id]
                        .columns()
                        .iter()
                        .map(|arr| take(arr, &take_arr, None))
                        .collect::<Result<Vec<_>, _>>().unwrap();

                    let is_completed = is_completed_col.remove(&batch_id).unwrap().finish();
                    let is_completed = Arc::new(is_completed) as ArrayRef;
                    let converted_steps = converted_steps_col.remove(&batch_id).unwrap().finish();
                    let converted_steps = Arc::new(converted_steps) as ArrayRef;
                    RecordBatch::try_new(out_schema.clone(), vec![vec![is_completed], vec![converted_steps], cols].concat())
                }).collect::<Vec<Result<_, _>>>().into_iter().collect::<Result<Vec<RecordBatch>, _>>().unwrap();

                for batch in batches.into_iter() {
                    print!("{}", arrow::util::pretty::pretty_format_batches(&[batch]).unwrap());
                }
            }
        }

        if let Some((batches, spans, skip)) = state.finalize().unwrap() {
            let res = funnel.evaluate(&batches, spans.clone(), skip).unwrap();
            println!("{:?}", res);

            let mut offset = skip;
            for (span, fr) in spans.into_iter().zip(res.into_iter()) {
                let (a, b) = abs_row_id(span + offset, &batches);
                println!("batch_id:{a}, row_id:{b}");
            }
        }
    }
}