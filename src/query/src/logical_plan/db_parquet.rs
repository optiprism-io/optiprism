use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_common::DFSchemaRef;
use datafusion_expr::Expr;
use datafusion_expr::LogicalPlan;
use datafusion_expr::UserDefinedLogicalNode;
use storage::db::OptiDBImpl;

use crate::error::QueryError;
use crate::Result;

pub struct DbParquetNode {
    pub db: Arc<OptiDBImpl>,
    pub projection: Vec<usize>,
    schema: DFSchemaRef,
}

impl Hash for DbParquetNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.projection.hash(state);
        self.schema.hash(state);
    }
}

impl Eq for DbParquetNode {}

impl PartialEq for DbParquetNode {
    fn eq(&self, other: &Self) -> bool {
        self.projection == other.projection && self.schema == other.schema
    }
}

impl DbParquetNode {
    pub fn try_new(db: Arc<OptiDBImpl>, projection: Vec<usize>) -> Result<Self> {
        let schema = db.schema1("events")?.project(&projection)?;
        Ok(Self {
            db,
            projection,
            schema: Arc::new(DFSchema::try_from(schema)?),
        })
    }
}

impl Debug for DbParquetNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for DbParquetNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "DbParquet"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DbParquet")
    }

    fn from_template(&self, _: &[Expr], _: &[LogicalPlan]) -> Arc<dyn UserDefinedLogicalNode> {
        Arc::new(
            DbParquetNode::try_new(self.db.clone(), self.projection.clone())
                .map_err(QueryError::into_datafusion_plan_error)
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
