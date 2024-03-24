use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query::TimeIntervalUnit;
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion::physical_plan::common::collect;
use datafusion_common::Column;
use datafusion_common::DFField;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::error::QueryError;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::SortField;
use crate::Result;

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct Funnel {
    pub ts_col: Column,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub window: Duration,
    pub steps: Vec<(Expr, StepOrder)>,
    pub exclude: Option<Vec<ExcludeExpr>>,
    pub constants: Option<Vec<Expr>>,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub partition_col: Expr,
    pub time_interval: Option<TimeIntervalUnit>,
    pub groups: Option<Vec<(Expr, String, SortField)>>,
}

impl Funnel {
    pub fn schema(&self, schema: &DFSchemaRef) -> DFSchemaRef {
        let mut fields = vec![
            DFField::new_unqualified("ts", DataType::Timestamp(TIME_UNIT, None), false),
            DFField::new_unqualified("total", DataType::Int64, false),
            DFField::new_unqualified("completed", DataType::Int64, false),
        ];

        if let Some(groups) = &self.groups {
            let group_fields = groups
                .iter()
                .map(|(expr, name, sort_field)| {
                    let f = schema.field_with_name(None, name.as_str()).unwrap();
                    DFField::new_unqualified(name, f.data_type().to_owned(), f.is_nullable())
                })
                .collect::<Vec<_>>();

            fields = [group_fields, fields].concat();
        }

        let mut step_fields = (0..self.steps.len())
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

        Arc::new(DFSchema::new_with_metadata(fields, Default::default()).unwrap())
    }
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum StepOrder {
    Exact,
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
    pub steps: Option<ExcludeSteps>,
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

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct FunnelNode {
    pub input: LogicalPlan,
    pub partition_inputs: Option<Vec<LogicalPlan>>,
    pub partition_col: Column,
    pub funnel: Funnel,
    pub schema: DFSchemaRef,
}

impl FunnelNode {
    pub fn new(
        input: LogicalPlan,
        partition_inputs: Option<Vec<LogicalPlan>>,
        partition_col: Column,
        funnel: Funnel,
    ) -> Result<Self> {
        let schema = funnel.schema(input.schema());
        let segment_field = DFField::new_unqualified("segment", DataType::Int64, false);
        let fields = vec![vec![segment_field], schema.fields().to_vec()].concat();
        let schema = Arc::new(DFSchema::new_with_metadata(fields, Default::default())?);
        Ok(Self {
            input,
            partition_inputs,
            partition_col,
            funnel,
            schema,
        })
    }
}
impl Debug for FunnelNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for FunnelNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "Funnel"
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
        write!(f, "Funnel")
    }

    fn from_template(&self, _: &[Expr], inputs: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            FunnelNode::new(
                inputs[0].to_owned(),
                self.partition_inputs.clone(),
                self.partition_col.clone(),
                self.funnel.clone(),
            )
            .unwrap(),
        )
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
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
