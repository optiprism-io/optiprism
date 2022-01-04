use crate::exprtree::segment::expressions::binary_op::BinaryOp;
use crate::exprtree::segment::expressions::expr as PhysicalSegmentExpr;
use crate::exprtree::segment::expressions::multibatch::sequence;
use arrow::datatypes::SchemaRef;
use chrono::Duration;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionContextState, QueryPlanner};
use datafusion::logical_plan::Column;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, Operator, UserDefinedLogicalNode};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::physical_plan::planner::{DefaultPhysicalPlanner, ExtensionPlanner};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub type JoinOn = (Column, Column);

#[derive(Clone)]
pub enum SegmentExpr {
    BinaryOp {
        left: Box<SegmentExpr>,
        op: Operator,
        right: Box<SegmentExpr>,
    },
    Count {
        predicate: Box<Expr>,
        op: Operator,
        right: i64,
    },
    Sum {
        predicate: Box<Expr>,
        left: Column,
        op: Operator,
        right: ScalarValue,
    },
    Sequence {
        schema: DFSchemaRef,
        ts_col: Column,
        window: Duration,
        steps: Vec<Expr>,
        exclude: Option<Vec<(Box<Expr>, Vec<usize>)>>,
        constants: Option<Vec<Column>>,
        filter: Option<sequence::Filter>,
    },
}

impl fmt::Debug for SegmentExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SegmentExpr::BinaryOp { left, op, right } => write!(f, "{:?} {} {:?}", left, op, right),
            SegmentExpr::Count {
                predicate,
                op,
                right,
            } => write!(f, "COUNT() {} {:?} WHERE {:?}", op, right, predicate),
            SegmentExpr::Sum {
                predicate,
                left,
                op,
                right,
            } => write!(
                f,
                "SUM({:?}) {:?} {} WHERE {:?}",
                left, op, right, predicate
            ),
            SegmentExpr::Sequence {
                schema,
                ts_col,
                window,
                steps,
                exclude,
                constants,
                filter,
            } => {
                write!(f, "SEQUENCE ts_col = {:?}, window = {:?}", ts_col, window);
                for (id, step) in steps.iter().enumerate() {
                    write!(f, " STEP #{}: {:?}", id, step);
                }

                if let Some(e) = exclude {
                    for (id, (expr, step_id)) in e.iter().enumerate() {
                        write!(f, " EXCLUDE {:?} FOR STEP #{:?}", expr, step_id);
                    }
                }

                if let Some(c) = constants {
                    write!(f, " CONSTANTS = {:?}", c);
                }

                if let Some(v) = filter {
                    write!(f, " FILTER = {:?}", v);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub left_expr: Option<Expr>,
    pub right_expr: Option<SegmentExpr>,
}

impl Segment {
    pub fn new(left_expr: Option<Expr>, right_expr: Option<SegmentExpr>) -> Self {
        Segment {
            left_expr,
            right_expr,
        }
    }
}

#[derive(Clone)]
pub struct JoinPlanNode {
    pub segments: Vec<Segment>,
    pub left: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    pub on: JoinOn,
    pub take_left_cols: Option<Vec<usize>>,
    pub take_right_cols: Option<Vec<usize>>,
    pub schema: DFSchemaRef,
    pub target_batch_size: usize,
}

impl Debug for JoinPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl JoinPlanNode {
    pub fn try_new(
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        on: JoinOn,
        segments: Vec<Segment>,
        schema: DFSchemaRef,
        target_batch_size: usize,
    ) -> Result<Self> {
        if schema.fields().len() == 0 {
            return Err(DataFusionError::Plan("empty schema".to_string()));
        }

        let mut take_left_cols: Vec<usize> = vec![];
        let mut take_right_cols: Vec<usize> = vec![];
        for field in schema.fields().iter() {
            if field.name().eq("segment") {
                continue;
            }
            match left.schema().index_of(field.name()) {
                Ok(idx) => take_left_cols.push(idx),
                _ => match right.schema().index_of(field.name()) {
                    Ok(idx) => take_right_cols.push(idx),
                    Err(_) => {
                        return Err(DataFusionError::Plan(format!(
                            "Column {} not found",
                            field.name()
                        )))
                    }
                },
            }
        }

        Ok(JoinPlanNode {
            segments,
            left,
            right,
            on: on.clone(),
            take_left_cols: if take_left_cols.is_empty() {
                None
            } else {
                Some(take_left_cols)
            },
            take_right_cols: if take_right_cols.is_empty() {
                None
            } else {
                Some(take_right_cols)
            },
            schema: schema.clone(),
            target_batch_size,
        })
    }
}

impl UserDefinedLogicalNode for JoinPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "SegmentJoin: {} = {}", self.on.0.name, self.on.1.name)?;
        for (id, segment) in self.segments.iter().enumerate() {
            if let Some(expr) = &segment.left_expr {
                writeln!(f, " #{} left: {:?}", id, expr)?;
            }
            if let Some(expr) = &segment.right_expr {
                writeln!(f, " #{} right: {:?}", id, expr)?;
            }
        }
        Ok(())
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        panic!("unimplemented");
    }
}
