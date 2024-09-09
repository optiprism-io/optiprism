use std::sync::Arc;
use std::sync::Mutex;
use arrow_row::SortField;
use datafusion::execution::context::ExecutionProps;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::ToDFSchema;
use datafusion_expr::LogicalPlan;
use crate::error::Result;
use crate::logical_plan;
use crate::logical_plan::funnel::FunnelNode;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Funnel;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::funnel::Options;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Count;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeExpr;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::ExcludeSteps;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Filter;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::StepOrder;
use crate::physical_plan::expressions::aggregate::partitioned::funnel::Touch;
use crate::physical_plan::funnel::FunnelFinalExec;
use crate::physical_plan::funnel::FunnelPartialExec;

pub(crate) fn build_funnel(
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
    node: &FunnelNode,
) -> Result<FunnelFinalExec> {
    let partition_inputs = node
        .partition_inputs
        .clone()
        .map(|c| physical_inputs[1..c.len()].to_vec());

    let partition_col = Column::new(
        node.partition_col.name.as_str(),
        logical_inputs[0]
            .schema()
            .index_of_column(&node.partition_col)?,
    );

    let f = node.funnel.clone();
    let ts_col = Column::new(
        node.partition_col.name.as_str(),
        logical_inputs[0].schema().index_of_column(&f.ts_col)?,
    );

    let schema = physical_inputs[0].schema();
    let dfschema = schema.clone().to_dfschema()?;
    let execution_props = ExecutionProps::new();
    let steps = f
        .steps
        .iter()
        .map(|(expr, order)| {
            let expr = create_physical_expr(expr, &dfschema, &execution_props).unwrap();
            let order = match order {
                logical_plan::funnel::StepOrder::Exact => StepOrder::Exact,
                logical_plan::funnel::StepOrder::Any(v) => StepOrder::Any(v.to_vec()),
            };
            (expr, order)
        })
        .collect::<Vec<_>>();

    let exclude = if let Some(exclude) = f.exclude {
        let mut out = vec![];
        for exclude in exclude {
            let expr = create_physical_expr(&exclude.expr, &dfschema, &execution_props)?;
            let steps = exclude.steps.map(|steps| ExcludeSteps {
                from: steps.from,
                to: steps.to,
            });

            out.push(ExcludeExpr { expr, steps })
        }
        Some(out)
    } else {
        None
    };

    let constants = if let Some(constants) = f.constants {
        Some(
            constants
                .iter()
                .map(|expr| create_physical_expr(expr, &dfschema, &execution_props))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )
    } else {
        None
    };

    let groups = if let Some(groups) = &f.groups {
        Some(
            groups
                .iter()
                .map(|(expr, name, sort_field)| {
                    let expr = create_physical_expr(expr, &dfschema, &execution_props)?;
                    let sf = SortField::new(sort_field.data_type.clone());
                    Ok((expr, name.clone(), sf))
                })
                .collect::<Result<Vec<_>>>()?,
        )
    } else {
        None
    };
    let opts = Options {
        schema: schema.to_owned(),
        ts_col: Arc::new(ts_col),
        from: f.from,
        to: f.to,
        window: f.window,
        steps,
        exclude,
        constants,
        count: match f.count {
            logical_plan::funnel::Count::Unique => Count::Unique,
            logical_plan::funnel::Count::NonUnique => Count::NonUnique,
            logical_plan::funnel::Count::Session => Count::Session,
        },
        filter: f.filter.map(|filter| match filter {
            logical_plan::funnel::Filter::DropOffOnAnyStep => Filter::DropOffOnAnyStep,
            logical_plan::funnel::Filter::DropOffOnStep(n) => Filter::DropOffOnStep(n),
            logical_plan::funnel::Filter::TimeToConvert(from, to) => {
                Filter::TimeToConvert(from, to)
            }
        }),
        touch: f.touch.map(|f| match f {
            logical_plan::funnel::Touch::First => Touch::First,
            logical_plan::funnel::Touch::Last => Touch::Last,
            logical_plan::funnel::Touch::Step(n) => Touch::Step(n),
        }),
        partition_col: Arc::new(partition_col.clone()),
        time_interval: f.time_interval,
        groups: groups.clone(),
    };
    let partial = FunnelPartialExec::try_new(
        physical_inputs[0].clone(),
        partition_inputs,
        partition_col,
        Arc::new(Mutex::new(Funnel::try_new(opts)?)),
    )?;

    FunnelFinalExec::try_new(
        Arc::new(partial),
        match &f.groups {
            None => 0,
            Some(g) => g.len(),
        },
        f.steps.len(),
    )
}
