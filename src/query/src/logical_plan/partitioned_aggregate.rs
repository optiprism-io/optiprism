use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Decimal128;
use arrow::datatypes::Field;
use arrow::datatypes::FieldRef;
use arrow::datatypes::Fields;
use arrow::datatypes::TimeUnit;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query;
use common::query::PartitionedAggregateFunction;
use common::types::RESERVED_COLUMN_AGG;
use common::types::RESERVED_COLUMN_AGG_PARTITIONED_AGGREGATE;
use common::types::RESERVED_COLUMN_AGG_PARTITIONED_COUNT;
use common::types::RESERVED_COLUMN_COUNT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;
use datafusion_expr::UserDefinedLogicalNodeCore;

use crate::error::QueryError;
use crate::logical_plan::SortField;
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
    // Funnel {
    // ts_col: Column,
    // from: DateTime<Utc>,
    // to: DateTime<Utc>,
    // window: Duration,
    // steps: Vec<(Expr, funnel::StepOrder)>,
    // exclude: Option<Vec<funnel::ExcludeExpr>>,
    // constants: Option<Vec<Column>>,
    // count: funnel::Count,
    // filter: Option<funnel::Filter>,
    // touch: funnel::Touch,
    // partition_col: Column,
    // bucket_size: Duration,
    // groups: Option<Vec<(Expr, SortField)>>,
    // },
}

impl AggregateExpr {
    pub fn change_predicate(self, partition_col: Column, predicate: Column) -> Self {
        match self {
            AggregateExpr::Count {
                filter,
                groups,
                predicate: _,
                partition_col: _,
                distinct,
            } => AggregateExpr::Count {
                filter,
                groups,
                predicate,
                partition_col,
                distinct,
            },
            AggregateExpr::Aggregate {
                filter,
                groups,
                partition_col: _,
                predicate: _,
                agg,
            } => AggregateExpr::Aggregate {
                filter,
                groups,
                partition_col,
                predicate,
                agg,
            },
            AggregateExpr::PartitionedCount {
                filter,
                outer_fn,
                groups,
                partition_col: _,
                distinct,
            } => AggregateExpr::PartitionedCount {
                filter,
                outer_fn,
                groups,
                partition_col,
                distinct,
            },
            AggregateExpr::PartitionedAggregate {
                filter,
                inner_fn,
                outer_fn,
                predicate: _,
                groups,
                partition_col: _,
            } => AggregateExpr::PartitionedAggregate {
                filter,
                inner_fn,
                outer_fn,
                predicate,
                groups,
                partition_col,
            },
            // AggregateExpr::Funnel {
            // ts_col,
            // from,
            // to,
            // window,
            // steps,
            // exclude,
            // constants,
            // count,
            // filter,
            // touch,
            // partition_col: _,
            // bucket_size,
            // groups,
            // } => AggregateExpr::Funnel {
            // ts_col,
            // from,
            // to,
            // window,
            // steps,
            // exclude,
            // constants,
            // count,
            // filter,
            // touch,
            // partition_col,
            // bucket_size,
            // groups,
            // },
        }
    }
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
            DataType::Int64
            | DataType::UInt64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _) => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
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
            // AggregateExpr::Funnel { groups, .. } => groups,
        };

        if let Some(groups) = groups {
            groups.iter().map(|(col, _)| col.clone()).collect()
        } else {
            vec![]
        }
    }

    pub fn field_names(&self) -> Vec<String> {
        match self {
            AggregateExpr::Count { .. } => vec![RESERVED_COLUMN_COUNT.to_string()],
            AggregateExpr::Aggregate { .. } => vec![RESERVED_COLUMN_AGG.to_string()],
            AggregateExpr::PartitionedCount { .. } => {
                vec![RESERVED_COLUMN_AGG_PARTITIONED_COUNT.to_string()]
            }
            AggregateExpr::PartitionedAggregate { .. } => {
                vec![RESERVED_COLUMN_AGG_PARTITIONED_AGGREGATE.to_string()]
            } // AggregateExpr::Funnel { .. } => unimplemented!(),
        }
    }
    pub fn fields(&self, schema: &DFSchema, final_: bool) -> Result<Vec<FieldRef>> {
        let fields = match self {
            AggregateExpr::Count { .. } => {
                vec![Field::new(RESERVED_COLUMN_COUNT, DataType::Int64, true)]
            }
            AggregateExpr::Aggregate { predicate, agg, .. } => {
                if final_ {
                    vec![Field::new(
                        RESERVED_COLUMN_AGG,
                        schema.field_from_column(predicate)?.data_type().to_owned(),
                        true,
                    )]
                } else {
                    vec![Field::new(
                        RESERVED_COLUMN_AGG,
                        return_type(
                            schema.field_from_column(predicate)?.data_type().to_owned(),
                            agg,
                            schema,
                        ),
                        true,
                    )]
                }
            }
            AggregateExpr::PartitionedCount { outer_fn, .. } => {
                let dt = match outer_fn {
                    AggregateFunction::Avg => DataType::Float64,
                    AggregateFunction::Count => DataType::Int64,
                    AggregateFunction::Min | AggregateFunction::Max => DataType::Int64,
                    _ => Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                };

                vec![Field::new(RESERVED_COLUMN_AGG_PARTITIONED_COUNT, dt, true)]
            }
            AggregateExpr::PartitionedAggregate {
                predicate,
                outer_fn,
                inner_fn,
                ..
            } => {
                let dt = schema.field_from_column(predicate)?.data_type();
                let rt1 = return_type(dt.to_owned(), inner_fn, schema);
                if final_ {
                    vec![Field::new(
                        RESERVED_COLUMN_AGG_PARTITIONED_AGGREGATE,
                        dt.to_owned(),
                        true,
                    )]
                } else {
                    let rt2 = return_type(rt1, outer_fn, schema);

                    vec![Field::new(
                        RESERVED_COLUMN_AGG_PARTITIONED_AGGREGATE,
                        rt2,
                        true,
                    )]
                }
            }
        };

        let fields = fields
            .iter()
            .map(|f| FieldRef::new(f.clone()))
            .collect::<Vec<_>>();

        Ok(fields)
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct PartitionedAggregatePartialNode {
    pub input: LogicalPlan,
    pub partition_inputs: Option<Vec<LogicalPlan>>,
    pub partition_col: Column,
    pub agg_expr: Vec<(AggregateExpr, String)>,
    pub schema: DFSchemaRef,
}

impl Debug for PartitionedAggregatePartialNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Eq")
    }
}

impl PartitionedAggregatePartialNode {
    pub fn try_new(
        input: LogicalPlan,
        partition_inputs: Option<Vec<LogicalPlan>>,
        partition_col: Column,
        agg_expr: Vec<(AggregateExpr, String)>,
    ) -> Result<Self> {
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<FieldRef> = Vec::new();
        let input_schema = input.schema();

        for (agg, name) in agg_expr.iter() {
            for group_expr in agg.group_exprs() {
                group_cols.insert(group_expr.display_name()?, ());
            }

            for f in agg.fields(input_schema, false)?.iter() {
                let f = Arc::new(Field::new(
                    format!("{}_{}", name, f.name()).as_str(),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                ));
                agg_result_fields.push(f.clone());
            }
        }

        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false));

        let group_fields = input_schema
            .fields()
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let group_fields = [vec![segment_field], group_fields].concat();
        let fields: Vec<FieldRef> = [group_fields, agg_result_fields].concat();

        let schema = DFSchema::from_unqualifed_fields(fields.into(), Default::default())?;
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

// impl Debug for PartitionedAggregatePartialNode {
// fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
// self.fmt_for_explain(f)
// }
// }
impl UserDefinedLogicalNodeCore for PartitionedAggregatePartialNode {
    fn name(&self) -> &str {
        "sdf"
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
        vec![Expr::Column(Column {
            relation: None,
            name: "project_id".to_string(),
        })]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionedAggregatePartial: ")?;
        for (expr, name) in &self.agg_expr {
            write!(f, ", agg: {:?} as {:?}", expr, name)?;
        }

        Ok(())
    }

    fn from_template(&self, _exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        PartitionedAggregatePartialNode::try_new(
            inputs[0].clone(),
            self.partition_inputs.clone(),
            self.partition_col.clone(),
            self.agg_expr.clone(),
        )
        .map_err(QueryError::into_datafusion_plan_error)
        .unwrap()
    }
}

#[derive(Hash, Eq, PartialEq)]
pub struct PartitionedAggregateFinalNode {
    pub input: LogicalPlan,
    pub agg_expr: Vec<(AggregateExpr, String)>,
    pub schema: DFSchemaRef,
}

impl crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode {
    pub fn try_new(input: LogicalPlan, agg_expr: Vec<(AggregateExpr, String)>) -> Result<Self> {
        let mut group_cols: HashMap<String, ()> = Default::default();
        let mut agg_result_fields: Vec<FieldRef> = Vec::new();
        let input_schema = input.schema();
        for (agg, name) in agg_expr.iter() {
            for group_expr in agg.group_exprs() {
                group_cols.insert(group_expr.display_name()?, ());
            }

            for f in agg.fields(input_schema, true)?.iter() {
                let f = Arc::new(Field::new(
                    format!("{}_{}", name, f.name()).as_str(),
                    f.data_type().to_owned(),
                    f.is_nullable(),
                ));
                agg_result_fields.push(f.clone());
            }
        }
        let segment_field = Arc::new(Field::new("segment", DataType::Int64, false));

        let group_fields = input_schema
            .fields()
            .iter()
            .filter(|f| group_cols.contains_key(f.name()))
            .cloned()
            .collect::<Vec<_>>();
        let group_fields = [vec![segment_field], group_fields].concat();
        let fields: Vec<FieldRef> = [group_fields, agg_result_fields].concat();
        let schema = DFSchema::from_unqualifed_fields(Fields::from(fields), Default::default())?;
        let ret = Self {
            input,
            agg_expr,
            schema: Arc::new(schema),
        };

        Ok(ret)
    }
}

impl Debug for crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode
    for crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PartitionedAggregateFinal"
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
        write!(f, "PartitionedAggregateFinal: ")?;
        for (expr, name) in &self.agg_expr {
            write!(f, ", agg: {:?} as {:?}", expr, name)?;
        }

        Ok(())
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        let node =
            crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode::try_new(
                inputs[0].clone(),
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
