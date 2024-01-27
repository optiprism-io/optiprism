use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
// use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::thread::spawn;

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
use arrow_row::SortField;
use axum::async_trait;
use crossbeam::channel::bounded;
use crossbeam::channel::Sender;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::expressions::Max;
use datafusion::physical_expr::Distribution;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec as DFAggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::AggregateExpr as DFAggregateExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::executor::block_on;
use futures::Stream;
use futures::StreamExt;
use metrics::histogram;
use tokio::time::Instant;

use crate::error::QueryError;
use crate::physical_plan::expressions::aggregate;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::segmentation;
use crate::Result;

// creates expression to combine batches from all the threads into one
macro_rules! combine_results {
    ($self:expr,$agg_idx:expr,$agg_field:expr,$agg_expr:expr,$groups:expr,$batches:expr,$res_batches:expr, $ty:ident) => {{
        // make agg function
        let agg_fn = match $agg_expr.op() {
            "count" => segmentation::aggregate::AggregateFunction::new_sum(),
            "min" => segmentation::aggregate::AggregateFunction::new_min(),
            "max" => segmentation::aggregate::AggregateFunction::new_max(),
            "sum" => segmentation::aggregate::AggregateFunction::new_sum(),
            "avg" => segmentation::aggregate::AggregateFunction::new_avg(),
            _ => unimplemented!(),
        };
        // make aggregate expression. Here predicate is an result from initial expression
        let mut agg = aggregate::Aggregate::<$ty, $ty>::try_new(
            None,
            Some($groups.clone()),
            Column::new_with_schema("segment", &$self.schema)?,
            Column::new_with_schema($agg_field.name(), &$self.schema)?,
            agg_fn,
        )
        .map_err(QueryError::into_datafusion_execution_error)?;

        for batch in $batches.iter() {
            agg.evaluate(batch, None)
                .map_err(QueryError::into_datafusion_execution_error)?;
        }
        let cols = agg
            .finalize()
            .map_err(QueryError::into_datafusion_execution_error)?;

        let schema = $self.agg_schemas[$agg_idx].clone();
        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false)) as FieldRef;
        let schema = Arc::new(Schema::new(
            vec![vec![segment_field], schema.fields().to_vec()].concat(),
        ));
        let batch = RecordBatch::try_new(schema, cols)?;
        let cols = $self
            .schema
            .fields()
            .iter()
            .map(
                |field| match batch.schema().index_of(field.name().as_str()) {
                    Ok(col_idx) => Ok(batch.column(col_idx).clone()),
                    Err(_) => {
                        let v = ScalarValue::try_from(field.data_type())?;
                        Ok(v.to_array_of_size(batch.column(0).len())?)
                    }
                },
            )
            .collect::<DFResult<Vec<ArrayRef>>>()?;
        let result = RecordBatch::try_new($self.schema.clone(), cols)?;
        $res_batches.push(result);
    }};
}

type NamedAggExpr = (Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>, String);
#[derive(Debug)]
pub struct SegmentedAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    partition_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
    partition_col: Column,
    agg_expr: Vec<NamedAggExpr>,
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
        agg_expr: Vec<NamedAggExpr>,
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
        let group_fields = [vec![segment_field], group_fields].concat();
        let fields: Vec<FieldRef> = [group_fields.clone(), agg_result_fields].concat();

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

// runner runs aggregation on single thread
struct RunnerOptions {
    partitions: Option<Arc<Vec<HashMap<i64, (), RandomState>>>>,
    // Segments<Aggs<>>
    agg_expr: Vec<Vec<AggExpr>>,
    // single partition input
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
}

// make a hashmap of all partition keys in segment
async fn collect_segment(
    mut partition_stream: SendableRecordBatchStream,
    partition_col: Column,
) -> Result<HashMap<i64, (), RandomState>> {
    let mut exist: HashMap<i64, (), RandomState> = ahash::HashMap::new();

    loop {
        match partition_stream.next().await {
            Some(Ok(batch)) => {
                let vals = partition_col
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?
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

type AggExpr = Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>;

impl DisplayAs for SegmentedAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentedAggregateExec")
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::HashPartitioned(vec![
            Arc::new(Column::new_with_schema("project_id", &self.input.schema()).unwrap()),
            Arc::new(Column::new_with_schema("user_id", &self.input.schema()).unwrap()),
        ])]
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
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut res: Vec<HashMap<i64, (), RandomState>> = Vec::new();
        let partitions = if let Some(inputs) = &self.partition_inputs {
            for input in inputs {
                let executed = input.execute(0, context.clone())?;
                let collected = block_on(collect_segment(executed, self.partition_col.clone()))
                    .map_err(QueryError::into_datafusion_execution_error)?;
                res.push(collected);
            }
            Some(Arc::new(res))
        } else {
            None
        };

        let partition_count = self.input.output_partitioning().partition_count();
        println!("{partition_count}");
        //        let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        // let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        // let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        // let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        // let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        // let a = block_on(self.input.execute(0, context.clone())?.next());
        // println!("{}", a.is_some());
        let runners = (0..partition_count)
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
                    agg_schemas: self.agg_schemas.clone(),
                    agg_expr: agg_expr.clone(),
                };
                Runner::new(opts, partition, context.clone())
            })
            .collect::<DFResult<Vec<_>>>()?;
        let (tx, rx) = bounded(partition_count);
        let mut completed = partition_count;
        let mut batches: Vec<RecordBatch> = Vec::with_capacity(partition_count);
        while let Ok(batch) = rx.recv() {
            batches.push(batch);
            completed -= 1;
            if completed == 0 {
                break;
            }
        }
        let groups = self
            .group_fields
            .iter()
            .map(|f| {
                (
                    col(f.name(), &self.schema).unwrap(),
                    f.name().to_owned(),
                    SortField::new(f.data_type().to_owned()),
                )
            })
            .collect::<Vec<_>>();

        let agg_fields = self.schema.fields()[self.group_fields.len()..].to_owned();
        let mut res_batches = vec![];
        for (agg_idx, (agg_expr, agg_field)) in
            self.agg_expr.iter().zip(agg_fields.iter()).enumerate()
        {
            let agg_expr = agg_expr.0.lock().unwrap();

            match agg_expr.fields()[0].data_type() {
                DataType::Int8 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        i8
                    )
                }
                DataType::Int16 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        i16
                    )
                }
                DataType::Int32 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        i32
                    )
                }
                DataType::Int64 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        i64
                    )
                }
                DataType::UInt8 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        u8
                    )
                }
                DataType::UInt16 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        u16
                    )
                }
                DataType::UInt32 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        u32
                    )
                }
                DataType::UInt64 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        u64
                    )
                }
                DataType::Float32 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        f32
                    )
                }
                DataType::Float64 => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        f64
                    )
                }
                DataType::Decimal128(_, _) => {
                    combine_results!(
                        self,
                        agg_idx,
                        agg_field,
                        agg_expr,
                        groups,
                        batches,
                        res_batches,
                        i128
                    )
                }
                _ => unimplemented!("{:?}", agg_expr.fields()[0].data_type()),
            };
        }
        let out = concat_batches(&self.schema, &res_batches)?;

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
            &[vec![out]],
            self.schema.clone(),
            None,
        )?);
        let partial_aggregate = Arc::new(DFAggregateExec::try_new(
            AggregateMode::Final,
            group_by,
            aggs,
            vec![None],
            input,
            self.schema.clone(),
        )?);

        let stream = partial_aggregate.execute(0, task_ctx)?;
        let batches = block_on(collect(stream))?;
        let _result = concat_batches(&self.schema, &batches)?;
        Ok(Box::pin(AggregateStream {
            is_ended: false,
            schema: self.schema.clone(),
            idx: 0,
            result: batches,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
