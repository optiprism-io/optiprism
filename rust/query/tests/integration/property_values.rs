#[cfg(test)]
mod tests {
    use arrow::util::pretty::print_batches;
    use common::types::{EventRef, PropValueOperation, PropertyRef};
    use common::ScalarValue;
    use datafusion::execution::context::SessionState;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::coalesce_batches::concat_batches;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use query::error::Result;
    use query::physical_plan::planner::QueryPlanner;
    use query::queries::property_values::{Filter, LogicalPlanBuilder, PropertyValues};
    use query::test_util::{create_entities, create_md, events_provider};
    use query::Context;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_property_values() -> Result<()> {
        let md = create_md()?;

        let org_id = 1;
        let proj_id = 1;

        let ctx = Context {
            organization_id: org_id,
            project_id: proj_id,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let input = events_provider(md.database.clone(), org_id, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::Event("Product Name".to_string()),
            event: Some(EventRef::RegularName("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::Like,
                value: Some(vec![ScalarValue::Utf8(Some("goo%".to_string()))]),
            }),
        };

        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, req).await?;
        println!("logical plan: {:?}", plan);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}));

        let exec_ctx = SessionContext::with_state(session_state);
        let physical_plan = exec_ctx.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;
        let concatenated = concat_batches(&result[0].schema(), &result, 0)?;

        print_batches(&[concatenated])?;
        Ok(())
    }
}
