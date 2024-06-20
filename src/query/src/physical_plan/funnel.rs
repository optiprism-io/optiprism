use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::result;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use ahash::RandomState;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::Datum;
use arrow::array::Decimal128Array;
use arrow::array::Decimal128Builder;
use arrow::array::Int64Array;
use arrow::array::Int64Builder;
use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::print_batches;
use arrow_row::OwnedRow;
use arrow_row::Row;
use arrow_row::RowConverter;
use arrow_row::SortField;
use async_trait::async_trait;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_PROJECT_ID;
use common::types::RESERVED_COLUMN_FUNNEL_AVG_TIME_TO_CONVERT;
use common::types::RESERVED_COLUMN_FUNNEL_COMPLETED;
use common::types::RESERVED_COLUMN_FUNNEL_CONVERSION_RATIO;
use common::types::RESERVED_COLUMN_FUNNEL_DROPPED_OFF;
use common::types::RESERVED_COLUMN_FUNNEL_DROP_OFF_RATIO;
use common::types::RESERVED_COLUMN_FUNNEL_TOTAL;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::expressions::Avg;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::expressions::Max;
use datafusion::physical_expr::{AggregateExpr, EquivalenceProperties};
use datafusion::physical_expr::Distribution;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::Partitioning::UnknownPartitioning;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::PhysicalSortRequirement;
use datafusion::physical_plan::aggregates::AggregateExec as DFAggregateExec;
use datafusion::physical_plan::aggregates::AggregateMode;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::AggregateExpr as DFAggregateExpr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::PlanProperties;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;

#[derive(Debug, Clone)]
enum Agg {
    Sum(i128),
    Avg(i128, i128),
}

impl Agg {
    pub fn result(&self) -> i128 {
        match self {
            Agg::Sum(sum) => *sum,
            Agg::Avg(sum, count) => *sum / *count,
        }
    }
}

struct Group {
    aggs: Vec<Agg>,
}

enum StaticArray {
    Int64(Int64Array),
    Decimal(Decimal128Array),
}

enum StaticArrayBuilder {
    Int64(Int64Builder),
    Decimal(Decimal128Builder),
}

fn aggregate(
    batch: &RecordBatch,
    mut groups: Vec<(PhysicalExprRef, SortField)>,
    aggs: Vec<(PhysicalExprRef, Agg)>,
) -> Result<Vec<ArrayRef>> {
    let is_groups = !groups.is_empty();
    let groups = groups
        .iter()
        .map(|(e, s)| {
            (
                e.evaluate(batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap(),
                s.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let row_converter = RowConverter::new(groups.iter().map(|(_, s)| s.clone()).collect())?;
    let mut groups_hash: HashMap<OwnedRow, Group> = HashMap::default();
    let mut single_group = Group {
        aggs: aggs
            .iter()
            .map(|(_, agg)| agg.to_owned())
            .collect::<Vec<_>>(),
    };

    let mut rows = if is_groups {
        let arrs = groups.iter().map(|(a, _)| a.to_owned()).collect::<Vec<_>>();

        Some(row_converter.convert_columns(&arrs)?)
    } else {
        None
    };

    let arrs = aggs
        .iter()
        .map(|(arr, agg)| {
            arr.evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
        })
        .collect::<Vec<_>>();
    // avoid downcast on each row
    let mut static_arrs = arrs
        .iter()
        .map(|arr| match arr.data_type() {
            DataType::Int64 => StaticArray::Int64(
                arr.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .to_owned(),
            ),

            DataType::Decimal128(_, _) => StaticArray::Decimal(
                arr.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .to_owned(),
            ),
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();

    let mut builders = arrs
        .iter()
        .map(|arr| match arr.data_type() {
            DataType::Int64 => {
                StaticArrayBuilder::Int64(Int64Builder::with_capacity(batch.num_rows()))
            }

            DataType::Decimal128(_, _) => {
                StaticArrayBuilder::Decimal(Decimal128Builder::with_capacity(batch.num_rows()))
            }
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();

    for row_id in 0..batch.num_rows() {
        let group = if let Some(rows) = rows.as_ref() {
            groups_hash
                .entry(rows.row(row_id).owned())
                .or_insert_with(|| {
                    let bucket = Group {
                        aggs: aggs
                            .iter()
                            .map(|(_, agg)| agg.to_owned())
                            .collect::<Vec<_>>(),
                    };
                    bucket
                })
        } else {
            &mut single_group
        };
        for (agg, arr) in group.aggs.iter_mut().zip(static_arrs.iter().map(|arr| arr)) {
            match arr {
                StaticArray::Int64(arr) => match agg {
                    Agg::Sum(sum) => {
                        *sum += arr.value(row_id) as i128;
                    }
                    Agg::Avg(sum, count) => {
                        *sum += arr.value(row_id) as i128;
                        *count += 1;
                    }
                },
                StaticArray::Decimal(arr) => match agg {
                    Agg::Sum(sum) => {
                        *sum += arr.value(row_id);
                    }
                    Agg::Avg(sum, count) => {
                        *sum += arr.value(row_id);
                        *count += 1;
                    }
                },
            }
        }
    }

    let cols = if let Some(rows) = &mut rows {
        let mut rows: Vec<Row> = Vec::with_capacity(groups_hash.len());
        for (row, group) in groups_hash.iter_mut() {
            rows.push(row.row());
            for (idx, agg) in group.aggs.iter().enumerate() {
                match &mut builders[idx] {
                    StaticArrayBuilder::Int64(b) => {
                        b.append_value(agg.result() as i64);
                    }
                    StaticArrayBuilder::Decimal(b) => {
                        b.append_value(agg.result());
                    }
                }
            }
        }

        let cols = builders
            .iter_mut()
            .map(|b| match b {
                StaticArrayBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
                StaticArrayBuilder::Decimal(b) => Arc::new(
                    b.finish()
                        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                        .unwrap(),
                ) as ArrayRef,
            })
            .collect::<Vec<_>>();

        let group_col = row_converter.convert_rows(rows)?;
        vec![group_col, cols].concat()
    } else {
        for (idx, agg) in single_group.aggs.iter().enumerate() {
            match &mut builders[idx] {
                StaticArrayBuilder::Int64(b) => {
                    b.append_value(agg.result() as i64);
                }
                StaticArrayBuilder::Decimal(b) => {
                    b.append_value(agg.result());
                }
            }
        }
        let cols = builders
            .iter_mut()
            .map(|b| match b {
                StaticArrayBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
                StaticArrayBuilder::Decimal(b) => Arc::new(
                    b.finish()
                        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                        .unwrap(),
                ) as ArrayRef,
            })
            .collect::<Vec<_>>();

        cols
    };

    Ok(cols)
}

#[derive(Eq, PartialEq, Clone, Debug)]
enum Stage {
    CollectSegments,
    CreateExpressions,
    PartialAggregate,
}

pub struct FunnelPartialExec {
    input: Arc<dyn ExecutionPlan>,
    segment_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
    partition_col: Column,
    expr: Arc<Mutex<Funnel>>,
    schema: SchemaRef,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl FunnelPartialExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_inputs: Option<Vec<Arc<dyn ExecutionPlan>>>,
        partition_col: Column,
        funnel: Arc<Mutex<Funnel>>,
    ) -> Result<Self> {
        let schema = funnel.lock().unwrap().schema();
        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false)) as FieldRef;
        let fields = vec![vec![segment_field], schema.fields().to_vec()].concat();
        let schema = Arc::new(Schema::new(fields));
        let cache = Self::compute_properties(&input, schema.clone())?;
        Ok(Self {
            input,
            segment_inputs: partition_inputs,
            partition_col,
            expr: funnel,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_properties(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<PlanProperties> {
        let eq_properties = EquivalenceProperties::new(schema);
        Ok(PlanProperties::new(
            eq_properties,
            UnknownPartitioning(input.output_partitioning().partition_count()),
            input.execution_mode(), // Execution Mode
        ))
    }
}

impl DisplayAs for FunnelPartialExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FunnelExec")
    }
}

impl Debug for FunnelPartialExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FunnelExec")
    }
}

#[async_trait]
impl ExecutionPlan for FunnelPartialExec {
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
            PhysicalSortRequirement {
                expr: col(COLUMN_CREATED_AT, &self.input.schema()).unwrap(),
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
            FunnelPartialExec::try_new(
                children[0].clone(),
                self.segment_inputs.clone(),
                self.partition_col.clone(),
                self.expr.clone(),
            )
                .map_err(QueryError::into_datafusion_execution_error)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
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
        Ok(Box::pin(PartialFunnelStream {
            stream: self.input.execute(partition, context.clone())?,
            segment_streams: RefCell::new(vec![]),
            schema: self.schema.clone(),
            partition_col: self.partition_col.clone(),
            expr: self.expr.clone(),
            out_expr: vec![],
            finished: false,
            segment_partitions: RefCell::new(segment_partitions),
            stage: Stage::CollectSegments,
        }))
    }
}

struct PartialFunnelStream {
    stream: SendableRecordBatchStream,
    segment_streams: RefCell<Vec<SendableRecordBatchStream>>,
    schema: SchemaRef,
    partition_col: Column,
    expr: Arc<Mutex<Funnel>>,
    out_expr: Vec<Arc<Mutex<Funnel>>>,
    finished: bool,
    segment_partitions: RefCell<Vec<HashMap<i64, (), RandomState>>>,
    stage: Stage,
}

impl RecordBatchStream for PartialFunnelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for PartialFunnelStream {
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
            self.out_expr = {
                let streams = self.segment_streams.borrow();
                if !streams.is_empty() {
                    (0..streams.len())
                        .map(|_| {
                            let expr = self.expr.lock().unwrap();
                            Arc::new(Mutex::new(expr.make_new().unwrap()))
                        })
                        .collect::<Vec<_>>()
                } else {
                    let expr = self.expr.lock().unwrap();
                    vec![Arc::new(Mutex::new(expr.make_new().unwrap()))]
                }
            };
            self.stage = Stage::PartialAggregate;
        }

        if self.stage == Stage::PartialAggregate {
            loop {
                match self.stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        for segment in 0..segments_count {
                            let mut agg = self.out_expr[segment].lock().unwrap();
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
                    Poll::Ready(None) => break,
                    Poll::Ready(other) => return Poll::Ready(other),
                    Poll::Pending => return Poll::Pending,
                }
            }
        }

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(10); // todo why 10?
        for segment in 0..segments_count {
            let mut aggr = self.out_expr[segment].lock().unwrap();
            let agg_cols = aggr
                .finalize()
                .map_err(QueryError::into_datafusion_execution_error)?;
            let seg_col =
                ScalarValue::Int64(Some(segment as i64)).to_array_of_size(agg_cols[0].len())?;
            let cols = [vec![seg_col], agg_cols].concat();
            let batch = RecordBatch::try_new(self.schema.clone(), cols)?;

            let cols = self
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

            let result = RecordBatch::try_new(self.schema.clone(), cols)?;
            batches.push(result);
        }
        let batch = concat_batches(&self.schema, batches.iter().collect::<Vec<_>>())?;

        self.finished = true;
        Poll::Ready(Some(Ok(batch)))
    }
}

#[derive(Debug)]
pub struct FunnelFinalExec {
    input: Arc<dyn ExecutionPlan>,
    groups: usize,
    steps: usize,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl FunnelFinalExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, groups: usize, steps: usize) -> Result<Self> {
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            input,
            groups,
            steps,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
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

impl DisplayAs for FunnelFinalExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FunnelFinalExec")
    }
}

#[async_trait]
impl ExecutionPlan for FunnelFinalExec {
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            FunnelFinalExec::try_new(children[0].clone(), self.groups, self.steps)
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

        Ok(Box::pin(FinalFunnelStream {
            inputs: RefCell::new(inputs),
            steps: self.steps,
            schema: self.schema(),
            final_batches: RefCell::new(vec![
                RecordBatch::new_empty(self.schema());
                partitions_count
            ]),
            finished: false,
            groups: self.groups,
        }))
    }
}

struct FinalFunnelStream {
    inputs: RefCell<Vec<SendableRecordBatchStream>>,
    schema: SchemaRef,
    final_batches: RefCell<Vec<RecordBatch>>,
    groups: usize,
    steps: usize,
    finished: bool,
}

impl Stream for FinalFunnelStream {
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

        // merge

        let mut aggs = vec![];
        for i in 0..self.steps {
            let cname = format!("step{}_total", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Sum(0)));

            let cname = format!("step{}_conversion_ratio", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Avg(0, 0)));

            let cname = format!("step{}_avg_time_to_convert", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Avg(0, 0)));

            let cname = format!("step{}_avg_time_to_convert_from_start", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Avg(0, 0)));

            let cname = format!("step{}_dropped_off", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Sum(0)));

            let cname = format!("step{}_drop_off_ratio", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Avg(0, 0)));

            let cname = format!("step{}_time_to_convert", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Sum(0)));
            let cname = format!("step{}_time_to_convert_from_start", i);
            aggs.push((col(&cname, &self.schema).unwrap(), Agg::Sum(0)));
        }
        let groups = self
            .schema
            .fields()
            .iter()
            .take(self.groups + 2) // segment+ts
            .map(|f| {
                (
                    col(f.name(), &self.schema).unwrap(),
                    SortField::new(f.data_type().to_owned()),
                )
            })
            .collect::<Vec<_>>();

        let final_batch = concat_batches(&self.schema, &self.final_batches.borrow().to_vec())?;
        let agg_arrs = aggregate(&final_batch, groups, aggs)
            .map_err(QueryError::into_datafusion_execution_error)?;
        let result = RecordBatch::try_new(self.schema.clone(), agg_arrs)?;
        self.finished = true;
        Poll::Ready(Some(Ok(result)))
    }
}

impl RecordBatchStream for FinalFunnelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow::array::RecordBatch;
    use arrow::datatypes::DataType;
    use arrow::util::pretty::print_batches;
    use arrow_row::SortField;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use common::query::TimeIntervalUnit;
    use datafusion::physical_expr::expressions::BinaryExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::expressions::Literal;
    use datafusion::physical_expr::PhysicalExprRef;
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use storage::test_util::parse_markdown_tables;

    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count::Unique;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeExpr;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
    use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
    use crate::physical_plan::funnel::FunnelFinalExec;
    use crate::physical_plan::funnel::FunnelPartialExec;

    #[tokio::test]
    async fn test_partial() {
        let data = r#"
| u(i64) | ts(ts)              | device(utf8) | v(i64) | c(i64) |
|--------|---------------------|--------------|--------|--------|
| 1      | 2022-08-29 22:10:57 | iphone       | 1      | 1      |
| 1      | 2022-08-29 22:11:57 | iphone       | 2      | 1      |
| 1      | 2022-08-29 22:12:57 | iphone       | 3      | 1      |
|        |                     |              |        |        |
| 1      | 2022-08-29 22:13:57 | android      | 1      | 1      |
| 1      | 2022-08-29 22:15:57 | android      | 2      | 1      |
| 1      | 2022-08-29 22:17:57 | android      | 3      | 1      |
|        |                     |              |        |        |
| 2      | 2022-08-29 22:10:57 | ios          | 1      | 1      |
|        |                     |              |        |        |
| 2      | 2022-08-29 22:11:57 | ios          | 2      | 1      |
| 2      | 2022-08-29 22:12:57 | ios          | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let input = MemoryExec::try_new(&[res], schema.clone(), None).unwrap();

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Exact)
        };

        let ex = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(4)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            Arc::new(expr) as PhysicalExprRef
        };

        let groups = vec![(
            Arc::new(Column::new_with_schema("device", &schema).unwrap()) as PhysicalExprRef,
            "device".to_string(),
            SortField::new(DataType::Utf8),
        )];
        let opts = Options {
            schema: schema.clone(),
            ts_col: Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            from: DateTime::parse_from_str("2022-08-29 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2022-08-29 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::minutes(200),
            steps: vec![e1, e2, e3],
            exclude: Some(vec![ExcludeExpr {
                expr: ex,
                steps: None,
            }]),
            // exclude: None,
            constants: None,
            // constants: Some(vec![Column::new_with_schema("c", &schema).unwrap()]),
            count: Unique,
            filter: None,
            touch: Touch::First,
            partition_col: Arc::new(Column::new_with_schema("u", &schema).unwrap()),
            time_interval: Some(TimeIntervalUnit::Hour),
            groups: Some(groups),
        };
        let mut f = Funnel::try_new(opts).unwrap();

        let exec = FunnelPartialExec::try_new(
            Arc::new(input),
            None,
            Column::new_with_schema("u", &schema).unwrap(),
            Arc::new(Mutex::new(f)),
        )
            .unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let result = collect(stream).await.unwrap();

        print_batches(&result).unwrap();
    }

    #[tokio::test]
    async fn test_final() {
        let p1 = r#"
| segment(i64) | device(utf8) | ts(ts) | step0_total(i64) | step0_conversion_ratio(decimal) | step0_avg_time_to_convert(decimal) | step0_dropped_off(i64) | step0_drop_off_ratio(decimal) | step0_time_to_convert(i64) | step0_time_to_convert_from_start(decimal) | step1_total(i64) | step1_conversion_ratio(decimal) | step1_avg_time_to_convert(decimal) | step1_dropped_off(i64) | step1_drop_off_ratio(decimal) | step1_time_to_convert(i64) | step1_time_to_convert_from_start(decimal) |
|--------------|--------------|--------|------------------|---------------------------------|------------------------------------|------------------------|-------------------------------|----------------------------|-------------------------------------------|------------------|---------------------------------|------------------------------------|------------------------|-------------------------------|----------------------------|-------------------------------------------|
| 1            | iphone       | 1      | 1                | 2                               | 2                                  | 2                      | 2                             | 2                          | 2                                         | 2                | 2                               | 2                                  | 2                      | 2                             | 2                          | 2                                         |
"#;

        let p2 = r#"
| segment(i64) | device(utf8) | ts(ts) | step0_total(i64) | step0_conversion_ratio(decimal) | step0_avg_time_to_convert(decimal) | step0_dropped_off(i64) | step0_drop_off_ratio(decimal) | step0_time_to_convert(i64) | step0_time_to_convert_from_start(decimal) | step1_total(i64) | step1_conversion_ratio(decimal) | step1_avg_time_to_convert(decimal) | step1_dropped_off(i64) | step1_drop_off_ratio(decimal) | step1_time_to_convert(i64) | step1_time_to_convert_from_start(decimal) |
|--------------|--------------|--------|------------------|---------------------------------|------------------------------------|------------------------|-------------------------------|----------------------------|-------------------------------------------|------------------|---------------------------------|------------------------------------|------------------------|-------------------------------|----------------------------|-------------------------------------------|
| 1            | iphone       | 1      | 1                | 1                               | 1                                  | 1                      | 1                             | 1                          | 1                                         | 1                | 1                               | 1                                  | 1                      | 1                             | 1                          | 1                                         |
"#;
        let res1 = parse_markdown_tables(p1).unwrap();
        let schema = res1[0].schema();

        let res2 = parse_markdown_tables(p2).unwrap();
        let input = MemoryExec::try_new(&[res1, res2], schema.clone(), None).unwrap();

        let exec = FunnelFinalExec::try_new(Arc::new(input), 1, 2).unwrap();
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let result = collect(stream).await.unwrap();

        print_batches(&result).unwrap();
    }
}
