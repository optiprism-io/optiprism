use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;

use crate::Result;

#[derive(Hash, Eq, PartialEq)]
pub struct LimitGroupsNode {
    input: LogicalPlan,
    pub skip: usize,
    pub groups: usize,
    pub limit: usize,
    schema: DFSchemaRef,
}

impl LimitGroupsNode {
    pub fn try_new(input: LogicalPlan, skip: usize, groups: usize, limit: usize) -> Result<Self> {
        let schema = input.schema().to_owned();

        Ok(Self {
            input,
            skip,
            groups,
            limit,
            schema,
        })
    }
}

impl Debug for LimitGroupsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for LimitGroupsNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "LimitGroups"
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
        write!(f, "LimitGroups")
    }

    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            LimitGroupsNode::try_new(inputs[0].clone(), self.skip, self.groups, self.limit)
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
