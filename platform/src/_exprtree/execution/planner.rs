use crate::exprtree::logical_plan::segment_join::{JoinPlanNode, SegmentExpr};
use crate::exprtree::physical_plan::segment_join::{JoinExec, Segment as PhysicalSegment};
use crate::exprtree::segment::expressions::boolean_op;
use crate::exprtree::segment::expressions::boolean_op::BooleanOp;
use crate::exprtree::segment::expressions::multibatch::binary_op::BinaryOp;
use crate::exprtree::segment::expressions::multibatch::count::Count;
use crate::exprtree::segment::expressions::multibatch::expr::Expr as PhysicalSegmentExpr;
use crate::exprtree::segment::expressions::multibatch::sequence::{Filter, Sequence};
use crate::exprtree::segment::expressions::multibatch::sum::Sum;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::{ExecutionContextState, QueryPlanner as DFQueryPlanner};
use datafusion::logical_plan::{DFSchema, LogicalPlan, Operator, UserDefinedLogicalNode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::planner::{
    DefaultPhysicalPlanner, ExtensionPlanner as DFExtensionPlanner,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

pub struct QueryPlanner {}

impl DFQueryPlanner for QueryPlanner {
    fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Teach the default physical planner how to plan TopK nodes.
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ExtensionPlanner {})]);
        // Delegate most work of physical planning to the default physical planner
        physical_planner.create_physical_plan(logical_plan, ctx_state)
    }
}

pub struct ExtensionPlanner {}

impl ExtensionPlanner {
    pub fn create_segment_expr(
        &self,
        expr: &SegmentExpr,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &ExecutionContextState,
    ) -> Result<Arc<dyn PhysicalSegmentExpr>> {
        match expr {
            SegmentExpr::BinaryOp { left, op, right } => match op {
                Operator::And => Ok(Arc::new(BinaryOp::<boolean_op::And>::new(
                    self.create_segment_expr(
                        left,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                    self.create_segment_expr(
                        right,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                ))),
                Operator::Or => Ok(Arc::new(BinaryOp::<boolean_op::Or>::new(
                    self.create_segment_expr(
                        left,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                    self.create_segment_expr(
                        right,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                ))),
                Operator::Eq => Ok(Arc::new(BinaryOp::<boolean_op::Eq>::new(
                    self.create_segment_expr(
                        left,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                    self.create_segment_expr(
                        right,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                ))),
                Operator::Gt => Ok(Arc::new(BinaryOp::<boolean_op::Gt>::new(
                    self.create_segment_expr(
                        left,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                    self.create_segment_expr(
                        right,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                ))),
                Operator::Lt => Ok(Arc::new(BinaryOp::<boolean_op::Lt>::new(
                    self.create_segment_expr(
                        left,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                    self.create_segment_expr(
                        right,
                        planner,
                        node,
                        logical_inputs,
                        physical_inputs,
                        ctx_state,
                    )?,
                ))),
                _ => panic!("unimplemented"),
            },
            SegmentExpr::Count {
                predicate,
                op,
                right,
            } => match op {
                Operator::Eq => Ok(Arc::new(Count::<boolean_op::Eq>::try_new(
                    physical_inputs[1].schema().as_ref(),
                    planner.create_physical_expr(
                        predicate,
                        &logical_inputs[1].schema(),
                        &physical_inputs[1].schema(),
                        ctx_state,
                    )?,
                    *right,
                )?)),
                Operator::Gt => Ok(Arc::new(Count::<boolean_op::Gt>::try_new(
                    physical_inputs[1].schema().as_ref(),
                    planner.create_physical_expr(
                        predicate,
                        &logical_inputs[1].schema(),
                        &physical_inputs[1].schema(),
                        ctx_state,
                    )?,
                    *right,
                )?)),
                Operator::Lt => Ok(Arc::new(Count::<boolean_op::Lt>::try_new(
                    physical_inputs[1].schema().as_ref(),
                    planner.create_physical_expr(
                        predicate,
                        &logical_inputs[1].schema(),
                        &physical_inputs[1].schema(),
                        ctx_state,
                    )?,
                    *right,
                )?)),
                _ => panic!("unimplemented"),
            },
            SegmentExpr::Sum {
                predicate,
                left,
                op,
                right,
            } => match (op, right) {
                (Operator::Eq, ScalarValue::Int8(Some(rv))) => {
                    Ok(Arc::new(Sum::<i8, i64, boolean_op::Eq>::try_new(
                        &physical_inputs[1].schema(),
                        Column::new_with_schema(left.name.as_str(), &physical_inputs[1].schema())?,
                        planner.create_physical_expr(
                            predicate,
                            &logical_inputs[1].schema(),
                            &physical_inputs[1].schema(),
                            ctx_state,
                        )?,
                        *rv as i64,
                    )?))
                }
                _ => panic!("unimplemented"),
            },
            SegmentExpr::Sequence {
                schema,
                ts_col,
                window,
                steps,
                exclude,
                constants,
                filter,
            } => {
                let physical_steps = steps
                    .iter()
                    .map(|expr| {
                        Ok(planner.create_physical_expr(
                            expr,
                            &logical_inputs[1].schema(),
                            &physical_inputs[1].schema(),
                            ctx_state,
                        )?)
                    })
                    .collect::<Result<Vec<Arc<dyn PhysicalExpr>>>>()?;

                let mut phys_seq = Sequence::try_new(
                    physical_inputs[1].schema().clone(),
                    Column::new_with_schema(ts_col.name.as_str(), &physical_inputs[1].schema())?,
                    window.clone(),
                    physical_steps,
                )?;

                if let Some(e) = exclude {
                    let physical_exclude = e
                        .iter()
                        .map(|(expr, steps)| {
                            Ok((
                                planner.create_physical_expr(
                                    expr,
                                    &logical_inputs[1].schema(),
                                    &physical_inputs[1].schema(),
                                    ctx_state,
                                )?,
                                steps.clone(),
                            ))
                        })
                        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, Vec<usize>)>>>()?;
                    phys_seq = phys_seq.with_exclude(physical_exclude)?;
                }

                if let Some(c) = constants {
                    phys_seq =
                        phys_seq.with_constants(c.iter().map(|x| x.name.as_str()).collect())?;
                }

                if let Some(f) = filter {
                    phys_seq = match f {
                        Filter::DropOffOnAnyStep => phys_seq.with_drop_off_on_any_step(),
                        Filter::DropOffOnStep(step_id) => {
                            phys_seq.with_drop_off_on_step(*step_id)?
                        }
                        Filter::TimeToConvert(from, to) => {
                            phys_seq.with_time_to_convert(*from, *to)?
                        }
                    };
                }

                Ok(Arc::new(phys_seq))
            }
        }
    }
}

impl DFExtensionPlanner for ExtensionPlanner {
    fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &ExecutionContextState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let any = node.as_any();
        let plan = if let Some(join) = any.downcast_ref::<JoinPlanNode>() {
            assert_eq!(logical_inputs.len(), 2, "Inconsistent number of inputs");

            let segments = join
                .segments
                .iter()
                .map(|x| {
                    let left_expr = if let Some(expr) = &x.left_expr {
                        Some(planner.create_physical_expr(
                            expr,
                            &logical_inputs[0].schema(),
                            &physical_inputs[0].schema(),
                            ctx_state,
                        )?)
                    } else {
                        None
                    };

                    let right_expr = if let Some(expr) = &x.right_expr {
                        Some(self.create_segment_expr(
                            expr,
                            planner,
                            node,
                            logical_inputs,
                            physical_inputs,
                            ctx_state,
                        )?)
                    } else {
                        None
                    };

                    Ok(PhysicalSegment {
                        left_expr,
                        right_expr,
                    })
                })
                .collect::<Result<Vec<PhysicalSegment>>>()?;
            Some(Arc::new(JoinExec::try_new(
                Arc::clone(&physical_inputs[0]),
                Arc::clone(&physical_inputs[1]),
                (
                    Column::new(
                        &join.on.0.name,
                        join.left.schema().index_of_column(&join.on.0)?,
                    ),
                    Column::new(
                        &join.on.1.name,
                        join.right.schema().index_of_column(&join.on.1)?,
                    ),
                ),
                segments,
                join.schema().as_ref().clone().into(),
                join.target_batch_size,
            )?) as Arc<dyn ExecutionPlan>)
        } else {
            None
        };
        Ok(plan)
    }
}
