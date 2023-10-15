use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Decimal128;
use arrow::datatypes::TimeUnit;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query;
use common::query::PartitionedAggregateFunction;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum AggregateFunction {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl From<&query::AggregateFunction> for AggregateFunction {
    fn from(value: &query::AggregateFunction) -> Self {
        match value {
            query::AggregateFunction::Count => AggregateFunction::Count,
            query::AggregateFunction::Sum => AggregateFunction::Sum,
            query::AggregateFunction::Min => AggregateFunction::Min,
            query::AggregateFunction::Max => AggregateFunction::Max,
            query::AggregateFunction::Avg => AggregateFunction::Avg,
            _ => panic!("Unsupported aggregate function: {:?}", value),
        }
    }
}

impl From<&query::PartitionedAggregateFunction> for AggregateFunction {
    fn from(value: &query::PartitionedAggregateFunction) -> Self {
        match value {
            PartitionedAggregateFunction::Count => AggregateFunction::Count,
            PartitionedAggregateFunction::Sum => AggregateFunction::Sum,
            PartitionedAggregateFunction::Avg => AggregateFunction::Avg,
            PartitionedAggregateFunction::Min => AggregateFunction::Min,
            PartitionedAggregateFunction::Max => AggregateFunction::Max,
        }
    }
}
pub mod funnel {
    use chrono::Duration;
    use datafusion_expr::Expr;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub enum StepOrder {
        Sequential,
        Any(Vec<(usize, usize)>), // any of the steps
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct ExcludeSteps {
        pub from: usize,
        pub to: usize,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct ExcludeExpr {
        pub expr: Expr,
        pub steps: Option<Vec<ExcludeSteps>>,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub enum Count {
        Unique,
        NonUnique,
        Session,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub enum Order {
        Any,
        Asc,
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub enum Filter {
        DropOffOnAnyStep,
        // funnel should fail on any step
        DropOffOnStep(usize),
        // funnel should fail on certain step
        TimeToConvert(Duration, Duration), // conversion should be within certain window
    }

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub enum Touch {
        First,
        Last,
        Step(usize),
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct SortField {
    pub data_type: DataType,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum AggregateExpr {
    Count {
        filter: Option<Expr>,
        groups: Option<Vec<(Expr, SortField)>>,
        predicate: Column,
        partition_col: Column,
        distinct: bool,
    },
    Aggregate {
        filter: Option<Expr>,
        groups: Option<Vec<(Expr, SortField)>>,
        partition_col: Column,
        predicate: Column,
        agg: AggregateFunction,
    },
    PartitionedCount {
        filter: Option<Expr>,
        outer_fn: AggregateFunction,
        groups: Option<Vec<(Expr, SortField)>>,
        partition_col: Column,
        distinct: bool,
    },
    PartitionedAggregate {
        filter: Option<Expr>,
        inner_fn: AggregateFunction,
        outer_fn: AggregateFunction,
        predicate: Column,
        groups: Option<Vec<(Expr, SortField)>>,
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
        groups: Option<Vec<(Expr, SortField)>>,
    },
}

fn return_type(dt: DataType, agg: &AggregateFunction, _schema: &DFSchema) -> DataType {
    match agg {
        AggregateFunction::Avg => DataType::Float64,
        AggregateFunction::Count => DataType::Int64,
        AggregateFunction::Min | AggregateFunction::Max => dt,
        AggregateFunction::Sum => match dt {
            DataType::Float32 | DataType::Float64 => DataType::Float64,
            DataType::Int8 | DataType::Int16 | DataType::Int32 => DataType::Int64,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::UInt64,
            DataType::Int64 | DataType::UInt64 | DataType::Decimal128(_, _) => {
                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
            }
            _ => unimplemented!("{dt:?}"),
        },
    }
}

impl AggregateExpr {
    pub fn group_exprs(&self) -> Vec<Expr> {
        let groups = match self {
            AggregateExpr::Count { groups, .. } => groups,
            AggregateExpr::Aggregate { groups, .. } => groups,
            AggregateExpr::PartitionedCount { groups, .. } => groups,
            AggregateExpr::PartitionedAggregate { groups, .. } => groups,
            AggregateExpr::Funnel { groups, .. } => groups,
        };

        if let Some(groups) = groups {
            groups.iter().map(|(col, _)| col.clone()).collect()
        } else {
            vec![]
        }
    }

    pub fn fields(&self, schema: &DFSchema) -> Result<Vec<DFField>> {
        let fields = match self {
            AggregateExpr::Count { .. } => {
                vec![DFField::new_unqualified("count", DataType::Int64, true)]
            }
            AggregateExpr::Aggregate { predicate, agg, .. } => {
                vec![DFField::new_unqualified(
                    "agg",
                    return_type(
                        schema.field_from_column(predicate)?.data_type().to_owned(),
                        agg,
                        schema,
                    ),
                    true,
                )]
            }
            AggregateExpr::PartitionedCount { outer_fn, .. } => {
                let dt = match outer_fn {
                    AggregateFunction::Avg => DataType::Float64,
                    AggregateFunction::Count => DataType::Int64,
                    AggregateFunction::Min | AggregateFunction::Max => DataType::Int64,
                    _ => Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                };

                vec![DFField::new_unqualified("partitioned_count", dt, true)]
            }
            AggregateExpr::PartitionedAggregate {
                predicate,
                outer_fn,
                inner_fn,
                ..
            } => {
                let dt = schema.field_from_column(predicate)?.data_type();
                let rt1 = return_type(dt.to_owned(), inner_fn, schema);
                let rt2 = return_type(rt1, outer_fn, schema);
                vec![DFField::new_unqualified("partitioned_agg", rt2, true)]
            }
            AggregateExpr::Funnel { groups, steps, .. } => {
                let mut fields = vec![
                    DFField::new_unqualified(
                        "ts",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                    DFField::new_unqualified("total", DataType::Int64, true),
                    DFField::new_unqualified("completed", DataType::Int64, true),
                ];

                // prepend group fields if we have grouping
                if let Some(groups) = &groups {
                    let group_fields = groups
                        .iter()
                        .map(|(expr, _)| {
                            schema
                                .field_with_unqualified_name(expr.display_name().unwrap().as_str())
                                .unwrap()
                                .to_owned()
                        })
                        .collect::<Vec<_>>();

                    fields = [group_fields, fields].concat();
                }

                // add fields for each step
                let mut step_fields = (0..steps.len())
                    .flat_map(|step_id| {
                        let fields = vec![
                            DFField::new_unqualified(
                                format!("step{}_total", step_id).as_str(),
                                DataType::Int64,
                                true,
                            ),
                            DFField::new_unqualified(
                                format!("step{}_time_to_convert", step_id).as_str(),
                                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                                true,
                            ),
                            DFField::new_unqualified(
                                format!("step{}_time_to_convert_from_start", step_id).as_str(),
                                DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                                true,
                            ),
                        ];
                        fields
                    })
                    .collect::<Vec<_>>();
                fields.append(&mut step_fields);

                fields
            }
        };

        Ok(fields)
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct PartitionedAggregateNode {
    pub input: LogicalPlan,
    pub partition_inputs: Option<Vec<LogicalPlan>>,
    pub partition_col: Column,
    pub agg_expr: Vec<(AggregateExpr, String)>,
    pub schema: DFSchemaRef,
}

impl PartitionedAggregateNode {
    pub fn try_new(
        input: LogicalPlan,
        partition_inputs: Option<Vec<LogicalPlan>>,
        partition_col: Column,
        agg_expr: Vec<(AggregateExpr, String)>,
    ) -> Result<Self> {
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<DFField> = Vec::new();
        let input_schema = input.schema();

        for (_agg_idx, (agg, name)) in agg_expr.iter().enumerate() {
            for group_expr in agg.group_exprs() {
                group_cols.insert(group_expr.display_name()?, ());
            }

            for f in agg.fields(input_schema)?.iter() {
                let f = DFField::new_unqualified(
                    format!("{}_{}", name, f.name()).as_str(),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                );
                agg_result_fields.push(f.clone());
            }
        }

        let segment_field = DFField::new_unqualified("segment", DataType::Int64, false);

        let group_fields = input_schema
            .fields()
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let group_fields = vec![vec![segment_field], group_fields].concat();
        let fields: Vec<DFField> = vec![group_fields, agg_result_fields].concat();
        let schema = DFSchema::new_with_metadata(fields, Default::default())?;
        let ret = Self {
            input,
            partition_inputs,
            partition_col,
            agg_expr,
            schema: Arc::new(schema),
        };
        Ok(ret)
    }
}

impl Debug for PartitionedAggregateNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for PartitionedAggregateNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PartitionedAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        let mut inputs = vec![];
        inputs.push(&self.input);
        if let Some(pi) = &self.partition_inputs {
            inputs.extend(pi.iter());
        }

        inputs
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionedAggregate: ")?;
        for (expr, name) in &self.agg_expr {
            write!(f, ", agg: {:?} as {:?}", expr, name)?;
        }

        Ok(())
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        let node = PartitionedAggregateNode::try_new(
            inputs[0].clone(),
            self.partition_inputs.clone(),
            self.partition_col.clone(),
            self.agg_expr.clone(),
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
