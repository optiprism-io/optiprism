use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::pin::Pin;
// use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

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
use arrow_row::SortField;
use axum::async_trait;
use common::types::COLUMN_PROJECT_ID;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::expressions::Max;
use datafusion::physical_expr::Distribution;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::aggregates::AggregateExec as DFAggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::AggregateExpr as DFAggregateExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::physical_plan::expressions::aggregate;
use crate::physical_plan::expressions::aggregate::PartitionedAggregateExpr;
use crate::physical_plan::expressions::segmentation;
use crate::Result;

// creates expression to combine batches from all the partitions into one
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
pub struct SegmentedAggregatePartialExec {
    input: Arc<dyn ExecutionPlan>,
    segment_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
    partition_col: Column,
    agg_expr: Vec<NamedAggExpr>,
    schema: SchemaRef,
    cache: PlanProperties,
    agg_schemas: Vec<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
}

impl SegmentedAggregatePartialExec {
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
            // let mut agg_fields = input_schema
            // .fields
            // .iter()
            // .filter(|f| agg_fields.contains(f))
            // .cloned()
            // .collect::<Vec<_>>();
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
        let schema = Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(&input, schema.clone())?;
        Ok(Self {
            input,
            segment_inputs: partition_inputs,
            partition_col,
            agg_expr,
            schema,
            cache,
            agg_schemas,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            UnknownPartitioning(input.output_partitioning().partition_count()), /* Output Partitioning */
            input.execution_mode(),                                             // Execution Mode
        ))
    }
}

impl DisplayAs for SegmentedAggregatePartialExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentedAggregatePartialExec")
    }
}

#[async_trait]
impl ExecutionPlan for SegmentedAggregatePartialExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        let sort = vec![
            PhysicalSortRequirement {
                expr: col(COLUMN_PROJECT_ID, &self.input.schema()).unwrap(),
                options: None,
            },
            PhysicalSortRequirement {
                expr: Arc::new(self.partition_col.clone()) as PhysicalExprRef,
                options: None,
            },
        ];
        vec![Some(sort); self.children().len()]
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // todo make it configurable, don't use project_id and user_id entities
        vec![Distribution::HashPartitioned(vec![
            Arc::new(Column::new_with_schema("project_id", &self.input.schema()).unwrap()),
            Arc::new(self.partition_col.clone()) as PhysicalExprRef,
        ])]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentedAggregatePartialExec::try_new(
                children[0].clone(),
                self.segment_inputs.clone(),
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
        let segment_streams: Vec<SendableRecordBatchStream> =
            if let Some(inputs) = &self.segment_inputs {
                inputs
                    .iter()
                    .map(|input| input.execute(partition, context.clone()))
                    .collect::<DFResult<_>>()?
            } else {
                vec![]
            };

        let segment_partitions = vec![HashMap::default(); segment_streams.len()];
        Ok(Box::pin(PartialAggregateStream {
            stream: self.input.execute(partition, context.clone())?,
            segment_streams: RefCell::new(segment_streams),
            schema: self.schema.clone(),
            partition_col: self.partition_col.clone(),
            named_agg_expr: self.agg_expr.clone(),
            agg_expr: vec![],
            agg_schemas: self.agg_schemas.clone(),
            finished: false,
            segment_partitions: RefCell::new(segment_partitions),
            stage: Stage::CollectSegments,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }
}

struct FinalAggregateStream {
    inputs: RefCell<Vec<SendableRecordBatchStream>>,
    schema: SchemaRef,
    named_agg_expr: Vec<NamedAggExpr>,
    group_fields: Vec<FieldRef>,
    // single partition input
    agg_schemas: Vec<SchemaRef>,
    final_batches: RefCell<Vec<RecordBatch>>,
    finished: bool,
}

impl RecordBatchStream for crate::physical_plan::segmented_aggregate::FinalAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum Stage {
    CollectSegments,
    CreateExpressions,
    PartialAggregate,
}

struct PartialAggregateStream {
    stream: SendableRecordBatchStream,
    segment_streams: RefCell<Vec<SendableRecordBatchStream>>,
    schema: SchemaRef,
    partition_col: Column,
    named_agg_expr: Vec<NamedAggExpr>,
    #[allow(clippy::type_complexity)]
    agg_expr: Vec<Vec<Arc<Mutex<Box<dyn PartitionedAggregateExpr>>>>>,
    // single partition input
    agg_schemas: Vec<SchemaRef>,
    finished: bool,
    segment_partitions: RefCell<Vec<HashMap<i64, (), RandomState>>>,
    stage: Stage,
}

impl RecordBatchStream for PartialAggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl Stream for PartialAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        if self.stage == Stage::CollectSegments {
            let partition_col = self.partition_col.clone();
            // collect segments
            {
                let mut streams = self.segment_streams.borrow_mut();
                for (id, stream) in streams.iter_mut().enumerate() {
                    loop {
                        match stream.poll_next_unpin(cx) {
                            Poll::Ready(Some(Ok(batch))) => {
                                let vals = partition_col
                                    .evaluate(&batch)?
                                    .into_array(batch.num_rows())?
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .clone();

                                for val in vals.iter() {
                                    self.segment_partitions.borrow_mut()[id]
                                        .insert(val.unwrap(), ());
                                }
                            }
                            Poll::Ready(None) => break,
                            other => return other,
                        }
                    }
                }
            }
            self.stage = Stage::CreateExpressions;
        }
        let segments_count = if self.segment_partitions.borrow().is_empty() {
            1
        } else {
            self.segment_partitions.borrow().len()
        };

        if self.stage == Stage::CreateExpressions {
            let agg_expr = {
                let streams = self.segment_streams.borrow();
                if !streams.is_empty() {
                    (0..streams.len())
                        .map(|_| {
                            self.named_agg_expr
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
                        self.named_agg_expr
                            .iter()
                            .map(|(e, _name)| {
                                let agg = e.lock().unwrap();
                                Arc::new(Mutex::new(agg.make_new().unwrap()))
                            })
                            .collect::<Vec<_>>(),
                    ]
                }
            };
            self.agg_expr = agg_expr;
            self.stage = Stage::PartialAggregate;
        }

        if self.stage == Stage::PartialAggregate {
            loop {
                match self.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        for segment in 0..segments_count {
                            for aggm in self.agg_expr[segment].iter() {
                                let mut agg = aggm.lock().unwrap();
                                if self.segment_partitions.borrow().len() > 0 {
                                    agg.evaluate(
                                        &batch,
                                        Some(&self.segment_partitions.borrow()[segment]),
                                    )
                                        .map_err(QueryError::into_datafusion_execution_error)?;
                                } else {
                                    agg.evaluate(&batch, None)
                                        .map_err(QueryError::into_datafusion_execution_error)?;
                                }
                            }
                        }
                    }
                    Poll::Ready(None) => break,
                    Poll::Ready(other) => return Poll::Ready(other),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(10); // todo why 10?
        for segment in 0..segments_count {
            for (agg_idx, aggrm) in self.agg_expr[segment].iter().enumerate() {
                let mut aggr = aggrm.lock().unwrap();
                let agg_cols = aggr
                    .finalize()
                    .map_err(QueryError::into_datafusion_execution_error)?;
                let seg_col =
                    ScalarValue::Int64(Some(segment as i64)).to_array_of_size(agg_cols[0].len())?;
                let cols = [vec![seg_col], agg_cols].concat();
                let schema = self.agg_schemas[agg_idx].clone();
                let schema = Arc::new(Schema::new(
                    [
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
                                let v = match field.data_type() {
                                    DataType::Int8 => ScalarValue::Int8(Some(0)),
                                    DataType::Int16 => ScalarValue::Int16(Some(0)),
                                    DataType::Int32 => ScalarValue::Int32(Some(0)),
                                    DataType::Int64 => ScalarValue::Int64(Some(0)),
                                    DataType::UInt8 => ScalarValue::UInt8(Some(0)),
                                    DataType::UInt16 => ScalarValue::UInt16(Some(0)),
                                    DataType::UInt32 => ScalarValue::UInt32(Some(0)),
                                    DataType::UInt64 => ScalarValue::UInt64(Some(0)),
                                    DataType::Decimal128(_, _) => ScalarValue::Decimal128(
                                        Some(0),
                                        DECIMAL_PRECISION,
                                        DECIMAL_SCALE,
                                    ),
                                    DataType::Float64 => ScalarValue::Float64(Some(0.)),
                                    _ => unimplemented!("{:?}", field.data_type()),
                                };
                                Ok(v.to_array_of_size(batch.column(0).len())?)
                            }
                        },
                    )
                    .collect::<DFResult<Vec<ArrayRef>>>()?;

                let result = RecordBatch::try_new(self.schema.clone(), cols)?;
                batches.push(result);
            }
        }
        let batch = concat_batches(&self.schema, batches.iter().collect::<Vec<_>>())?;

        self.finished = true;
        Poll::Ready(Some(Ok(batch)))
    }
}

#[derive(Debug)]
pub struct SegmentedAggregateFinalExec {
    input: Arc<dyn ExecutionPlan>,
    agg_expr: Vec<NamedAggExpr>,
    schema: SchemaRef,
    agg_schemas: Vec<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    group_fields: Vec<FieldRef>,
}

impl DisplayAs for SegmentedAggregateFinalExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SegmentedAggregateFinalExec")
    }
}

#[async_trait]
impl ExecutionPlan for SegmentedAggregateFinalExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SegmentedAggregateFinalExec::try_new(children[0].clone(), self.agg_expr.clone())
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partitions_count = self.input.output_partitioning().partition_count();

        let inputs = (0..partitions_count)
            .map(|pid| self.input.execute(pid, context.clone()))
            .collect::<DFResult<_>>()?;
        Ok(Box::pin(FinalAggregateStream {
            inputs: RefCell::new(inputs),
            schema: self.schema.clone(),
            named_agg_expr: self.agg_expr.clone(),
            group_fields: self.group_fields.clone(),
            agg_schemas: self.agg_schemas.clone(),
            final_batches: RefCell::new(vec![
                RecordBatch::new_empty(self.schema.clone());
                partitions_count
            ]),
            finished: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema.as_ref()))
    }
}

impl crate::physical_plan::segmented_aggregate::SegmentedAggregateFinalExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, agg_expr: Vec<NamedAggExpr>) -> Result<Self> {
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
            //            let agg_fields = input_schema
            // .fields
            // .iter()
            // .filter(|f| agg_fields.contains(f))
            // .cloned()
            // .collect::<Vec<_>>();
            //
            // dbg!(agg_fields.clone());
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
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            input,
            agg_expr,
            schema: Arc::new(schema),
            agg_schemas,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            group_fields,
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> Result<PlanProperties> {
        let eq_properties = input.equivalence_properties().clone();

        Ok(PlanProperties::new(
            eq_properties,
            UnknownPartitioning(1),
            input.execution_mode(),
        ))
    }
}

#[async_trait]
impl Stream for FinalAggregateStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        for (idx, input) in self.inputs.borrow_mut().iter_mut().enumerate() {
            match input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.final_batches.borrow_mut()[idx] = batch;
                }
                Poll::Ready(None) => {}
                Poll::Pending => return Poll::Pending,
                Poll::Ready(other) => return Poll::Ready(other),
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
        for (agg_idx, (agg_expr, agg_field)) in self
            .named_agg_expr
            .iter()
            .zip(agg_fields.iter())
            .enumerate()
        {
            let agg_expr = agg_expr.0.lock().unwrap();
            let batches = &self.final_batches.borrow();
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
            vec![None;self.agg_schemas.len()],
            input,
            self.schema.clone(),
        )?);

        let mut stream = partial_aggregate.execute(0, task_ctx)?;
        let mut out_batches = vec![];
        loop {
            match stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => out_batches.push(batch),
                Poll::Pending => {}
                Poll::Ready(None) => break,
                other => return other,
            }
        }
        let result = concat_batches(&self.schema, &out_batches)?;
        self.finished = true;
        Poll::Ready(Some(Ok(result)))
    }
}
