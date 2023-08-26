use std::any::Any;
use std::collections::HashMap;
use std::default;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_row::SortField;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_common::Result as DFResult;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

pub mod funnel {
    use chrono::Duration;
    use datafusion_expr::Expr;

    pub enum StepOrder {
        Sequential,
        Any(Vec<(usize, usize)>), // any of the steps
    }

    pub struct ExcludeSteps {
        from: usize,
        to: usize,
    }

    pub struct ExcludeExpr {
        pub expr: Expr,
        pub steps: Option<Vec<ExcludeSteps>>,
    }

    pub enum Count {
        Unique,
        NonUnique,
        Session,
    }

    pub enum Order {
        Any,
        Asc,
    }

    pub enum Filter {
        DropOffOnAnyStep,
        // funnel should fail on any step
        DropOffOnStep(usize),
        // funnel should fail on certain step
        TimeToConvert(Duration, Duration), // conversion should be within certain window
    }

    pub enum Touch {
        First,
        Last,
        Step(usize),
    }
}

enum AggregateExpr {
    Count {
        filter: Option<Expr>,
        groups: Option<Vec<Column, SortField>>,
        predicate: Column,
        partition_col: Column,
        distinct: bool,
    },
    Aggregate {
        filter: Option<Expr>,
        groups: Option<Vec<Column, SortField>>,
        partition_col: Column,
        predicate: Column,
        agg: AggregateFunction,
    },
    PartitionedCount {
        filter: Option<Expr>,
        outer_fn: AggregateFunction,
        groups: Option<Vec<Column, SortField>>,
        partition_col: Column,
        distinct: bool,
    },
    PartitionedAggregate {
        filter: Option<Expr>,
        inner_fn: AggregateFunction,
        outer_fn: AggregateFunction,
        predicate: Column,
        groups: Option<Vec<Column, SortField>>,
        partition_col: Column,
    },
    Funnel {
        ts_col: Column,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        window: Duration,
        steps: Vec<(Expr, funnel::StepOrder)>,
        exclude: Option<Vec<funnel::ExcludeExpr>>,
        constants: Option<Vec<Column>>,
        count: funnel::Count,
        filter: Option<funnel::Filter>,
        touch: funnel::Touch,
        partition_col: Column,
        bucket_size: Duration,
        groups: Option<Vec<Column, SortField>>,
    },
}

#[derive(Hash, Eq, PartialEq)]
pub struct SegmentedAggregateNode {
    input: LogicalPlan,
    partition_inputs: Option<Vec<LogicalPlan>>,
    pub partition_col: Column,
    pub agg_expr: Vec<AggregateExpr>,
    pub agg_aliases: Vec<String>,
    schema: DFSchemaRef,
}

impl SegmentedAggregateNode {
    pub fn try_new(
        input: LogicalPlan,
        partition_inputs: Vec<LogicalPlan>,
        partition_col: Column,
        agg_expr: Vec<AggregateExpr>,
        agg_aliases: Vec<String>,
    ) -> Result<Self> {
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<DFField> = Vec::new();
        let input_schema = input.schema();

        for (agg_idx, agg) in agg_expr.iter().enumerate() {
            for group_col in agg.group_columns() {
                group_cols.insert(group_col.name().to_string(), ());
            }

            for f in agg.fields().iter() {
                let f = DFField::new(
                    None,
                    format!("{}_{}", agg_aliases[agg_idx], f.name()).as_str(),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                );
                agg_result_fields.push(f.clone().into());
            }
        }

        let segment_field =
            Arc::new(DFField::new(None, "segment", DataType::Int64, false)) as DFField;

        let group_fields = input_schema
            .fields()
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let group_fields = vec![vec![segment_field], group_fields.clone()].concat();
        let fields: Vec<DFField> = vec![group_fields.clone(), agg_result_fields].concat();
        let schema = DFSchema::new_with_metadata(fields, Default::default())?;
        let ret = Self {
            input,
            partition_inputs: Some(partition_inputs),
            partition_col,
            agg_expr,
            agg_aliases,
            schema: Arc::new(schema),
        };
        Ok(ret)
    }
}

impl Debug for SegmentedAggregateNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for SegmentedAggregateNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "SegmentedAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SegmentedAggregate")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        let node = SegmentedAggregateNode::try_new(
            inputs[0].clone(),
            self.partition_inputs.clone().unwrap(),
            self.partition_col.clone(),
            self.agg_expr.clone(),
            self.agg_aliases.clone(),
        )
        .map_err(QueryError::into_datafusion_plan_error)
        .unwrap();

        Arc::new(node)
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        use std::hash::Hash;
        let mut s = state;
        self.hash(&mut s);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        match other.as_any().downcast_ref::<Self>() {
            Some(o) => self == o,

            None => false,
        }
    }
}
