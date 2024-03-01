use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::util::pretty::print_batches;
use async_trait::async_trait;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use common::types::RESERVED_COLUMN_FUNNEL_COMPLETED;
use common::types::RESERVED_COLUMN_FUNNEL_TOTAL;
use datafusion::execution::RecordBatchStream;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::expressions::Avg;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::expressions::Max;
use datafusion::physical_expr::expressions::Sum;
use datafusion::physical_expr::AggregateExpr;
use datafusion::physical_expr::Distribution;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_expr::PhysicalExpr;
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
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use futures::Stream;
use futures::StreamExt;

use crate::error::QueryError;
use crate::error::Result;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;

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
        let schema = Schema::new(fields);

        Ok(Self {
            input,
            segment_inputs: partition_inputs,
            partition_col,
            expr: funnel,
            schema: Arc::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl Debug for FunnelPartialExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl DisplayAs for FunnelPartialExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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
                expr: col(COLUMN_USER_ID, &self.input.schema()).unwrap(),
                options: None,
            },
        ];
        vec![Some(sort); self.children().len()]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.input.output_partitioning().partition_count())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // todo make it configurable, don't use project_id and user_id entities
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
    metrics: ExecutionPlanMetricsSet,
}

impl FunnelFinalExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, groups: usize, steps: usize) -> Result<Self> {
        Ok(Self {
            input,
            groups,
            steps,
            metrics: ExecutionPlanMetricsSet::new(),
        })
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

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
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

        let groups = self
            .schema
            .fields()
            .iter()
            .take(self.groups + 2) // segment+ts
            .map(|f| (col(f.name(), &self.schema).unwrap(), f.name().to_owned()))
            .collect::<Vec<_>>();
        // merge

        let mut aggs = vec![];
        aggs.push(Arc::new(Sum::new(
            col(RESERVED_COLUMN_FUNNEL_TOTAL, &self.schema).unwrap(),
            RESERVED_COLUMN_FUNNEL_TOTAL,
            self.schema
                .field_with_name(RESERVED_COLUMN_FUNNEL_TOTAL)
                .unwrap()
                .data_type()
                .clone(),
        )) as Arc<dyn DFAggregateExpr>);
        aggs.push(Arc::new(Sum::new(
            col(RESERVED_COLUMN_FUNNEL_COMPLETED, &self.schema).unwrap(),
            RESERVED_COLUMN_FUNNEL_COMPLETED,
            self.schema
                .field_with_name(RESERVED_COLUMN_FUNNEL_COMPLETED)
                .unwrap()
                .data_type()
                .clone(),
        )) as Arc<dyn DFAggregateExpr>);

        for i in 0..self.steps {
            let cname = format!("step{}_total", i);
            aggs.push(Arc::new(Sum::new(
                col(&cname, &self.schema).unwrap(),
                cname.clone(),
                self.schema
                    .field_with_name(&cname)
                    .unwrap()
                    .data_type()
                    .clone(),
            )) as Arc<dyn DFAggregateExpr>);
            let cname = format!("step{}_time_to_convert", i);
            aggs.push(Arc::new(Sum::new(
                col(&cname, &self.schema).unwrap(),
                cname.clone(),
                self.schema
                    .field_with_name(&cname)
                    .unwrap()
                    .data_type()
                    .clone(),
            )) as Arc<dyn DFAggregateExpr>);
            let cname = format!("step{}_time_to_convert_from_start", i);
            aggs.push(Arc::new(Sum::new(
                col(&cname, &self.schema).unwrap(),
                cname.clone(),
                self.schema
                    .field_with_name(&cname)
                    .unwrap()
                    .data_type()
                    .clone(),
            )) as Arc<dyn DFAggregateExpr>);
        }
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let group_by = PhysicalGroupBy::new_single(groups);
        let input = Arc::new(MemoryExec::try_new(
            &[self.final_batches.borrow().clone()],
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

        // todo return multiple batches
        let result = concat_batches(&out_batches[0].schema(), &out_batches)?;

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
| 1      | 2020-04-12 22:10:57 | iphone       | 1      | 1      |
| 1      | 2020-04-12 22:11:57 | iphone       | 2      | 1      |
| 1      | 2020-04-12 22:12:57 | iphone       | 3      | 1      |
|        |                     |              |        |        |
| 1      | 2020-04-12 22:13:57 | android      | 1      | 1      |
| 1      | 2020-04-12 22:15:57 | android      | 2      | 1      |
| 1      | 2020-04-12 22:17:57 | android      | 3      | 1      |
|        |                     |              |        |        |
| 2      | 2020-04-12 22:10:57 | ios          | 1      | 1      |
|        |                     |              |        |        |
| 2      | 2020-04-12 22:11:57 | ios          | 2      | 1      |
| 2      | 2020-04-12 22:12:57 | ios          | 3      | 1      |
"#;
        let res = parse_markdown_tables(data).unwrap();
        let schema = res[0].schema();
        let input = MemoryExec::try_new(&[res], schema.clone(), None).unwrap();

        let e1 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(1)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e2 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(2)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
        };
        let e3 = {
            let l = Column::new_with_schema("v", &schema).unwrap();
            let r = Literal::new(ScalarValue::Int64(Some(3)));
            let expr = BinaryExpr::new(Arc::new(l), Operator::Eq, Arc::new(r));
            (Arc::new(expr) as PhysicalExprRef, StepOrder::Sequential)
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
            ts_col: Column::new_with_schema("ts", &schema).unwrap(),
            from: DateTime::parse_from_str("2020-04-12 22:10:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            to: DateTime::parse_from_str("2020-04-12 22:21:57 +0000", "%Y-%m-%d %H:%M:%S %z")
                .unwrap()
                .with_timezone(&Utc),
            window: Duration::milliseconds(200),
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
            partition_col: Column::new_with_schema("u", &schema).unwrap(),
            bucket_size: Duration::days(2),
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
|segment(i64)  | device(utf8)  | ts(ts)                  | total(i64) | completed(i64) | step0_total(i64) | step0_time_to_convert(decimal) | step0_time_to_convert_from_start(decimal) | step1_total(i64) | step1_time_to_convert(decimal) | step1_time_to_convert_from_start(decimal) |
|--------------|---------------|-------------------------|------------|----------------|------------------|--------------------------------|-------------------------------------------|------------------|--------------------------------|-------------------------------------------|
|1             | iphone        | 1                       | 1          | 1              | 1                | 1                              | 1                                         | 1                | 1                              |      1                                    |
|1             | android       | 1                       | 1          | 1              | 1                | 1                              | 1                                         | 1                | 1                              |      1                                    |
"#;

        let p2 = r#"
|segment(i64)   | device(utf8)  | ts(ts)                  | total(i64) | completed(i64) | step0_total(i64) | step0_time_to_convert(decimal) | step0_time_to_convert_from_start(decimal) | step1_total(i64) | step1_time_to_convert(decimal) | step1_time_to_convert_from_start(decimal) |
|---------------|---------------|-------------------------|------------|----------------|------------------|--------------------------------|-------------------------------------------|------------------|--------------------------------|-------------------------------------------|
|1              | iphone        | 1                       | 1          | 1              | 1                | 1                              | 1                                         | 1                | 1                              |      1                                    |
|1              | ios           | 1                       | 1          | 1              | 1                | 1                              | 1                                         | 1                | 1                              |      1                                    |
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
