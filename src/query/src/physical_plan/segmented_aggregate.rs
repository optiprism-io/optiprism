use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::mpsc;
// use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::thread::sleep;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Duration;

use ahash::HashMapExt;
use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use axum::async_trait;
use crossbeam::channel::bounded;
use crossbeam::channel::unbounded;
use crossbeam::channel::Sender;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::expressions::Max;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec as DFAggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::AggregateExpr as DFAggregateExpr;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::executor::block_on;
use futures::Stream;
use futures::StreamExt;
use tokio::join;
use tokio::task;

use crate::error::QueryError;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::Result;

#[derive(Debug)]
pub struct SegmentedAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    partition_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
    partition_col: Column,
    agg_expr: Vec<(Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>, String)>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
    group_fields: Vec<FieldRef>,
}

impl SegmentedAggregateExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
        partition_col: Column,
        agg_expr: Vec<(Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>, String)>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let mut agg_schemas: Vec<SchemaRef> = Vec::new();
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<FieldRef> = Vec::new();

        for (agg, agg_name) in agg_expr.iter() {
            let mut agg_fields: Vec<FieldRef> = Vec::new();
            let agg = agg.lock().unwrap();

            for (_expr, col_name) in agg.group_columns() {
                group_cols.insert(col_name.clone(), ());
                let f = input_schema.field_with_name(col_name.as_str())?;

                agg_fields.push(Arc::new(f.to_owned()));
            }

            for f in agg.fields().iter() {
                let f = Field::new(
                    format!("{}_{}", agg_name, f.name()),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                );
                agg_result_fields.push(f.clone().into());
                agg_fields.push(f.into());
            }
            agg_schemas.push(Arc::new(Schema::new(agg_fields)));
        }

        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false)) as FieldRef;
        let group_fields = input_schema
            .fields
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let group_fields = vec![vec![segment_field], group_fields].concat();
        let fields: Vec<FieldRef> = vec![group_fields.clone(), agg_result_fields].concat();

        let schema = Schema::new(fields);
        Ok(Self {
            input,
            partition_inputs,
            partition_col,
            agg_expr,
            schema: Arc::new(schema),
            agg_schemas,
            metrics: ExecutionPlanMetricsSet::new(),
            group_fields,
        })
    }
}

struct RunnerOptions {
    partitions: Option<Arc<Vec<HashMap<i64, (), RandomState>>>>,
    agg_expr: Vec<Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>>,
    input: Arc<dyn ExecutionPlan>,
    group_fields: Vec<FieldRef>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    baseline_metrics: BaselineMetrics,
}

async fn collect_partition(
    mut partition_stream: SendableRecordBatchStream,
    partition_col: Column,
) -> Result<HashMap<i64, (), RandomState>> {
    let mut exist: HashMap<i64, (), RandomState> = ahash::HashMap::new();

    loop {
        match partition_stream.next().await {
            Some(Ok(batch)) => {
                let vals = partition_col
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();

                for val in vals.iter() {
                    exist.insert(val.unwrap(), ());
                }
            }

            None => {
                break;
            }
            _ => unreachable!(),
        }
    }

    Ok(exist)
}

struct Runner {
    // tx: Sender<RecordBatch>,
    partitions: Option<Arc<Vec<HashMap<i64, (), RandomState>>>>,
    agg_expr: Vec<Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>>,
    input: SendableRecordBatchStream,
    group_fields: Vec<FieldRef>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    baseline_metrics: BaselineMetrics,
}

impl Runner {
    fn new(
        opts: RunnerOptions,
        partition: usize,
        // tx: Sender<RecordBatch>,
        ctx: Arc<TaskContext>,
    ) -> DFResult<Self> {
        let res = Self {
            // tx,
            partitions: opts.partitions,
            agg_expr: opts.agg_expr,
            input: opts.input.execute(partition, ctx)?,
            group_fields: opts.group_fields.clone(),
            schema: opts.schema.clone(),
            agg_schemas: opts.agg_schemas.clone(),
            baseline_metrics: opts.baseline_metrics.clone(),
        };

        Ok(res)
    }

    fn run(&mut self, tx: Sender<RecordBatch>) -> Result<()> {
        let segments_count = if let Some(streams) = &self.partitions {
            streams.len()
        } else {
            1
        };

        loop {
            match block_on(self.input.next()) {
                Some(Ok(batch)) => {
                    println!("orig");
                    print_batches(&[batch.clone()]).unwrap();
                    println!("/orig");
                    for segment in 0..segments_count {
                        for aggm in self.agg_expr[segment].iter() {
                            let mut agg = aggm.lock().unwrap();

                            if let Some(exist) = &self.partitions {
                                agg.evaluate(&batch, Some(&exist[segment]))?;
                            } else {
                                agg.evaluate(&batch, None)?;
                            }
                        }
                    }
                }
                None => {
                    break;
                }
                Some(Err(er)) => {
                    return Err(QueryError::from(er));
                }
            };
        }

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(10); // todo why 10?
        for segment in 0..segments_count {
            for (agg_idx, aggrm) in self.agg_expr[segment].iter().enumerate() {
                let mut aggr = aggrm.lock().unwrap();
                let agg_cols = aggr
                    .finalize()
                    .map_err(QueryError::into_datafusion_execution_error)?;
                println!("AGG COLS {:?}", agg_cols);
                let seg_col =
                    ScalarValue::Int64(Some(segment as i64)).to_array_of_size(agg_cols[0].len());
                let cols = vec![vec![seg_col], agg_cols].concat();
                let schema = self.agg_schemas[agg_idx].clone();
                let schema = Arc::new(Schema::new(
                    vec![
                        vec![Arc::new(Field::new("segment", DataType::Int64, false))],
                        schema.fields().to_vec(),
                    ]
                    .concat(),
                ));
                let batch = RecordBatch::try_new(schema, cols)?;
                let cols = self
                    .schema
                    .fields()
                    .iter()
                    .map(
                        |field| match batch.schema().index_of(field.name().as_str()) {
                            Ok(col_idx) => Ok(batch.column(col_idx).clone()),
                            Err(_) => {
                                let v = ScalarValue::try_from(field.data_type())?;
                                Ok(v.to_array_of_size(batch.column(0).len()))
                            }
                        },
                    )
                    .collect::<DFResult<Vec<ArrayRef>>>()?;

                let result = RecordBatch::try_new(self.schema.clone(), cols)?;
                batches.push(result);
            }
        }
        let batch = concat_batches(&self.schema, batches.iter().collect::<Vec<_>>())?;

        // merge
        let agg_fields = self.schema.fields()[self.group_fields.len()..].to_owned();
        let aggs: Vec<Arc<dyn DFAggregateExpr>> = agg_fields
            .iter()
            .map(|f| {
                Arc::new(Max::new(
                    col(f.name(), &self.schema).unwrap(),
                    f.name().to_owned(),
                    f.data_type().to_owned(),
                )) as Arc<dyn DFAggregateExpr>
            })
            .collect::<Vec<_>>();

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let group_by_expr = self
            .group_fields
            .iter()
            .map(|f| (col(f.name(), &self.schema).unwrap(), f.name().to_owned()))
            .collect::<Vec<_>>();

        let group_by = PhysicalGroupBy::new_single(group_by_expr);
        let input = Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema.clone(),
            None,
        )?);
        let partial_aggregate = Arc::new(DFAggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggs,
            vec![None],
            vec![None],
            input,
            self.schema.clone(),
        )?);

        let stream = partial_aggregate.execute(0, task_ctx)?;
        let result = block_on(collect(stream))?;
        let batch = concat_batches(&self.schema, &result)?;
        tx.send(batch)
            .map_err(|err| QueryError::Internal(err.to_string()))
    }
}

fn run(runners: Vec<Runner>, tx: Sender<RecordBatch>) {
    for mut runner in runners.into_iter() {
        println!("RUN");
        let tx = tx.clone();
        spawn(move || runner.run(tx).unwrap());
    }
}

#[async_trait]
impl ExecutionPlan for SegmentedAggregateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentedAggregateExec::try_new(
                children[0].clone(),
                self.partition_inputs.clone(),
                self.partition_col.clone(),
                self.agg_expr.clone(),
            )
            .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut res: Vec<HashMap<i64, (), RandomState>> = Vec::new();
        let partitions = if let Some(inputs) = &self.partition_inputs {
            for input in inputs {
                let executed = input.execute(0, context.clone())?;
                let collected = block_on(collect_partition(executed, self.partition_col.clone()))
                    .map_err(QueryError::into_datafusion_execution_error)?;
                res.push(collected);
            }
            Some(Arc::new(res))
        } else {
            None
        };

        let partition_count = self.input.output_partitioning().partition_count();

        let runners = (0..partition_count)
            .into_iter()
            .map(|partition| {
                let agg_expr = if let Some(inputs) = &self.partition_inputs {
                    (0..inputs.len())
                        .map(|_| {
                            self.agg_expr
                                .iter()
                                .map(|(e, _name)| {
                                    let agg = e.lock().unwrap();
                                    Arc::new(Mutex::new(agg.make_new().unwrap()))
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![
                        self.agg_expr
                            .iter()
                            .map(|(e, _name)| {
                                let agg = e.lock().unwrap();
                                Arc::new(Mutex::new(agg.make_new().unwrap()))
                            })
                            .collect::<Vec<_>>(),
                    ]
                };

                let opts = RunnerOptions {
                    partitions: partitions.clone(),
                    input: self.input.clone(),
                    schema: self.schema.clone(),
                    group_fields: self.group_fields.clone(),
                    agg_schemas: self.agg_schemas.clone(),
                    agg_expr: agg_expr.clone(),
                    baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
                };
                Runner::new(opts, partition, context.clone())
            })
            .collect::<DFResult<Vec<_>>>()?;
        let (tx, rx) = bounded(5);
        // let (tx, rx) = mpsc::channel();
        run(runners, tx);

        let mut completed = partition_count;
        let mut result: Vec<RecordBatch> = Vec::with_capacity(partition_count);
        while let batch = rx.recv().unwrap() {
            print_batches(&[batch.clone()]).unwrap();
            result.push(batch);
            completed -= 1;
            if completed == 0 {
                break;
            }
        }

        Ok(Box::pin(AggregateStream {
            is_ended: false,
            schema: self.schema.clone(),
            idx: 0,
            result,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentedAggregateExec")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct AggregateStream {
    is_ended: bool,
    schema: SchemaRef,
    idx: usize,
    result: Vec<RecordBatch>,
}

impl RecordBatchStream for AggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for AggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.is_ended {
            return Poll::Ready(None);
        }
        if self.idx == self.result.len() - 1 {
            self.is_ended = true;
        }

        let res = self.result[self.idx].clone();
        self.idx += 1;
        Poll::Ready(Some(Ok(res)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow::datatypes::DataType;
    use arrow::util::pretty::print_batches;
    use arrow_row::SortField;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    pub use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use store::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate;
    use crate::physical_plan::expressions::aggregate::partitioned::count;
    use crate::physical_plan::expressions::aggregate::partitioned::count::PartitionedCount;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
    use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
    // use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
    // use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
    // use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::Unique;
    // use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
    // use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
    use crate::physical_plan::expressions::segmentation::aggregate::AggregateFunction;
    use crate::physical_plan::segmented_aggregate::SegmentedAggregateExec;

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | country(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|---------------|--------|--------|-------------|
| 0            | iphone       | Spain         | 1      | 2020-04-12 22:10:57      | e1          |
| 0            | iphone       | Spain         | 0      | 2020-04-12 22:11:57      | e2          |
| 0            | iphone       | Spain         | 0      | 2020-04-12 22:12:57      | e3          |
| 0            | android      | Spain         | 1      | 2020-04-12 22:13:57      | e1          |
| 0            | android      | Spain         | 1      | 2020-04-12 22:14:57      | e2          |
| 0            | android      | Spain         | 0      | 2020-04-12 22:15:57      | e3          |
| 1            | osx          | Germany       | 1      | 2020-04-12 22:10:57      | e1          |
| 1            | osx          | Germany       | 1      | 2020-04-12 22:11:57      | e2          |
| 1            | osx          | Germany       | 0      | 2020-04-12 22:12:57      | e3          |
| 1            | osx          | UK            | 0      | 2020-04-12 22:13:57      | e1          |
| 1            | osx          | UK            | 0      | 2020-04-12 22:14:57      | e2          |
| 1            | osx          | UK            | 0      | 2020-04-12 22:15:57      | e3          |
| 2            | osx          | Portugal      | 1      | 2020-04-12 22:10:57      | e1          |
| 2            | osx          | Portugal      | 1      | 2020-04-12 22:11:57      | e2          |
| 2            | osx          | Portugal      | 0      | 2020-04-12 22:12:57      | e3          |
| 2            | osx          | Spain         | 0      | 2020-04-12 22:13:57      | e1          |
| 2            | osx          | Spain         | 0      | 2020-04-12 22:14:57      | e2          |
| 2            | osx          | Spain         | 0      | 2020-04-12 22:15:57      | e3          |
| 3            | osx          | Spain         | 1      | 2020-04-12 22:10:57     | e1          |
| 3            | osx          | Spain         | 1      | 2020-04-12 22:12:57      | e2          |
| 3            | osx          | Spain         | 0      | 2020-04-12 22:12:57      | e3          |
| 3            | osx          | UK            | 0      | 2020-04-12 22:13:57      | e1          |
| 3            | osx          | UK            | 0      | 2020-04-12 22:14:57      | e2          |
| 3            | osx          | UK            | 0      | 2020-04-12 22:15:57      | e3          |
| 4            | osx          | Russia        | 0      | 2020-04-12 22:10:57      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&[batches], schema.clone(), None)?;

        let pagg1 = {
            // let groups = vec![(
            // Column::new_with_schema("device", &schema).unwrap(),
            // SortField::new(DataType::Utf8),
            // )];
            let groups = vec![
                (
                    Arc::new(Column::new_with_schema("country", &schema).unwrap())
                        as PhysicalExprRef,
                    "country".to_string(),
                    SortField::new(DataType::Utf8),
                ),
                // SortField::new(DataType::Utf8),
            ];
            let count = PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_sum(),
                Some(groups),
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pagg2 = {
            let groups = vec![
                (
                    Arc::new(Column::new_with_schema("country", &schema).unwrap())
                        as PhysicalExprRef,
                    "country".to_string(),
                    SortField::new(DataType::Utf8),
                ),
                // (
                // Column::new_with_schema("device", &schema).unwrap(),
                // SortField::new(DataType::Utf8),
                // ),
            ];
            let count = PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_sum(),
                Some(groups),
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pagg3 = {
            let _groups = vec![(
                Column::new_with_schema("country", &schema).unwrap(),
                SortField::new(DataType::Utf8),
            )];
            let count = PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_sum(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pagg4 = {
            /*let groups = vec![
                (
                    Column::new_with_schema("country", &schema).unwrap(),
                    SortField::new(DataType::Utf8),
                ),
                // SortField::new(DataType::Utf8),
            ];*/
            let e1 = {
                let l = Column::new_with_schema("event", &schema).unwrap();
                let r = Literal::new(ScalarValue::Utf8(Some("e1".to_string())));
                let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
                (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
            };
            let e2 = {
                let l = Column::new_with_schema("event", &schema).unwrap();
                let r = Literal::new(ScalarValue::Utf8(Some("e2".to_string())));
                let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
                (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
            };
            let e3 = {
                let l = Column::new_with_schema("event", &schema).unwrap();
                let r = Literal::new(ScalarValue::Utf8(Some("e3".to_string())));
                let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
                (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
            };

            let opts = Options {
                schema: schema.clone(),
                ts_col: Column::new_with_schema("ts", &schema).unwrap(),
                from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                    .unwrap()
                    .with_timezone(&Utc),
                to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                    .unwrap()
                    .with_timezone(&Utc),
                window: Duration::seconds(100),
                steps: vec![e1, e2, e3],
                exclude: None,
                // exclude: None,
                constants: None,
                // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
                count: Unique,
                filter: None,
                touch: Touch::First,
                partition_col: Column::new_with_schema("user_id", &schema).unwrap(),
                bucket_size: Duration::days(100),
                groups: None,
            };
            let f = Funnel::try_new(opts).unwrap();

            Arc::new(Mutex::new(Box::new(f) as Box<dyn PartitionedAggregateExpr>))
        };

        let agg1 = aggregate::count::Count::<i64>::try_new(
            None,
            None,
            Column::new_with_schema("v", &schema).unwrap(),
            Column::new_with_schema("user_id", &schema).unwrap(),
            false,
        )
        .unwrap();
        let agg1 = Arc::new(Mutex::new(
            Box::new(agg1) as Box<dyn PartitionedAggregateExpr>
        ));

        let pinput1 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
||
| 3            |
| 4            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            MemoryExec::try_new(&[batches], schema, None)?
        };

        let pinput2 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
| 1            |
| 2            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            MemoryExec::try_new(&[batches], schema, None)?
        };

        let seg = SegmentedAggregateExec::try_new(
            Arc::new(input),
            Some(vec![Arc::new(pinput2), Arc::new(pinput1)]),
            Column::new_with_schema("user_id", &schema).unwrap(),
            vec![
                (pagg1, "count".to_string()),
                (pagg2, "min".to_string()),
                (pagg3, "min2".to_string()),
                (pagg4, "f1".to_string()),
                (agg1, "f2".to_string()),
            ],
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_ungrouped() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | country(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|---------------|--------|--------|-------------|
| 0            | iphone       | Spain         | 1      | 1      | e1          |
| 0            | iphone       | Spain         | 0      | 2      | e2          |
| 0            | iphone       | Spain         | 0      | 3      | e3          |
| 0            | android      | Spain         | 1      | 4      | e1          |
| 0            | android      | Spain         | 1      | 5      | e2          |
|||||||
| 0            | android      | Spain         | 0      | 6      | e3          |
| 1            | osx          | Germany       | 1      | 1      | e1          |
| 1            | osx          | Germany       | 1      | 2      | e2          |
| 1            | osx          | Germany       | 0      | 3      | e3          |
| 1            | osx          | UK            | 0      | 4      | e1          |
| 1            | osx          | UK            | 0      | 5      | e2          |
|||||||
| 1            | osx          | UK            | 0      | 6      | e3          |
| 2            | osx          | Portugal      | 1      | 1      | e1          |
| 2            | osx          | Portugal      | 1      | 2      | e2          |
| 2            | osx          | Portugal      | 0      | 3      | e3          |
| 2            | osx          | Spain         | 0      | 4      | e1          |
| 2            | osx          | Spain         | 0      | 5      | e2          |
| 2            | osx          | Spain         | 0      | 6      | e3          |
| 3            | osx          | Spain         | 1      | 1      | e1          |
| 3            | osx          | Spain         | 1      | 2      | e2          |
|||||||
| 3            | osx          | Spain         | 0      | 3      | e3          |
| 3            | osx          | UK            | 0      | 4      | e1          |
| 3            | osx          | UK            | 0      | 5      | e2          |
| 3            | osx          | UK            | 0      | 6      | e3          |
| 4            | osx          | Russia        | 0      | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&[batches], schema.clone(), None)?;

        let agg1 = {
            let count = count::PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_avg(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let agg2 = {
            let groups = vec![
                (
                    Arc::new(Column::new_with_schema("country", &schema).unwrap())
                        as PhysicalExprRef,
                    "country".to_string(),
                    SortField::new(DataType::Utf8),
                ),
                (
                    Arc::new(Column::new_with_schema("device", &schema).unwrap())
                        as PhysicalExprRef,
                    "device".to_string(),
                    SortField::new(DataType::Utf8),
                ),
            ];
            let count = PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_sum(),
                Some(groups),
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let pinput1 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
||
| 3            |
| 4            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            MemoryExec::try_new(&[batches], schema, None)?
        };

        let pinput2 = {
            let data = r#"
| user_id(i64) |
|--------------|
| 0            |
| 1            |
| 2            |
"#;

            let batches = parse_markdown_tables(data).unwrap();
            let schema = batches[0].schema();
            MemoryExec::try_new(&[batches], schema, None)?
        };

        let seg = SegmentedAggregateExec::try_new(
            Arc::new(input),
            Some(vec![Arc::new(pinput2), Arc::new(pinput1)]),
            Column::new_with_schema("user_id", &schema).unwrap(),
            vec![(agg1, "count".to_string()), (agg2, "min".to_string())],
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_no_segments() -> anyhow::Result<()> {
        let data = r#"
| user_id(i64) | device(utf8) | country(utf8) | v(i64) | ts(ts) | event(utf8) |
|--------------|--------------|---------------|--------|--------|-------------|
| 0            | iphone       | Spain         | 1      | 1      | e1          |
| 0            | iphone       | Spain         | 0      | 2      | e2          |
| 0            | iphone       | Spain         | 0      | 3      | e3          |
| 0            | android      | Spain         | 1      | 4      | e1          |
| 0            | android      | Spain         | 1      | 5      | e2          |
|||||||
| 0            | android      | Spain         | 0      | 6      | e3          |
| 1            | osx          | Germany       | 1      | 1      | e1          |
| 1            | osx          | Germany       | 1      | 2      | e2          |
| 1            | osx          | Germany       | 0      | 3      | e3          |
| 1            | osx          | UK            | 0      | 4      | e1          |
| 1            | osx          | UK            | 0      | 5      | e2          |
|||||||
| 1            | osx          | UK            | 0      | 6      | e3          |
| 2            | osx          | Portugal      | 1      | 1      | e1          |
| 2            | osx          | Portugal      | 1      | 2      | e2          |
| 2            | osx          | Portugal      | 0      | 3      | e3          |
| 2            | osx          | Spain         | 0      | 4      | e1          |
| 2            | osx          | Spain         | 0      | 5      | e2          |
| 2            | osx          | Spain         | 0      | 6      | e3          |
| 3            | osx          | Spain         | 1      | 1      | e1          |
| 3            | osx          | Spain         | 1      | 2      | e2          |
|||||||
| 3            | osx          | Spain         | 0      | 3      | e3          |
| 3            | osx          | UK            | 0      | 4      | e1          |
| 3            | osx          | UK            | 0      | 5      | e2          |
| 3            | osx          | UK            | 0      | 6      | e3          |
| 4            | osx          | Russia        | 0      | 6      | e3          |
"#;

        let batches = parse_markdown_tables(data).unwrap();
        let schema = batches[0].schema();
        let input = MemoryExec::try_new(&[batches], schema.clone(), None)?;

        let agg1 = {
            let count = count::PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_avg(),
                None,
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let agg2 = {
            let groups = vec![
                (
                    Arc::new(Column::new_with_schema("country", &schema).unwrap())
                        as PhysicalExprRef,
                    "country".to_string(),
                    SortField::new(DataType::Utf8),
                ),
                (
                    Arc::new(Column::new_with_schema("device", &schema).unwrap())
                        as PhysicalExprRef,
                    "device".to_string(),
                    SortField::new(DataType::Utf8),
                ),
            ];
            let count = PartitionedCount::<i64>::try_new(
                None,
                AggregateFunction::new_sum(),
                Some(groups),
                Column::new_with_schema("user_id", &schema).unwrap(),
                false,
            )
            .unwrap();

            Arc::new(Mutex::new(
                Box::new(count) as Box<dyn PartitionedAggregateExpr>
            ))
        };

        let seg = SegmentedAggregateExec::try_new(
            Arc::new(input),
            None,
            Column::new_with_schema("user_id", &schema).unwrap(),
            vec![(agg1, "count".to_string()), (agg2, "min".to_string())],
        )?;

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = seg.execute(0, task_ctx)?;
        let result = collect(stream).await?;

        print_batches(&result).unwrap();
        Ok(())
    }
}
