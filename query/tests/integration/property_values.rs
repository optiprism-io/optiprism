#[cfg(test)]
mod tests {
    use arrow::util::pretty::print_batches;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::physical_plan::coalesce_batches::concat_batches;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{ExecutionConfig, ExecutionContext};
    use datafusion_commonValue;
    use query::error::Result;
    use query::physical_plan::planner::QueryPlanner;
    use query::queries::property_values::{Filter, LogicalPlanBuilder, PropertyValues};
    use query::queries::types::{EventRef, PropValueOperation, PropertyRef};
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
            account_id: 1,
            project_id: proj_id,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let input = events_provider(md.database.clone(), org_id, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::Event("Product Name".to_string()),
            event: Some(EventRef::Regular("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::Like,
                value: Some(vec![ScalarValue::Utf8(Some("goo%".to_string()))]),
            }),
        };

        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, req).await?;
        println!("logical plan: {:?}", plan);
        let config = ExecutionConfig::new()
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_target_partitions(1);

        let ctx = ExecutionContext::with_config(config);
        let physical_plan = ctx.create_physical_plan(&plan).await?;
        let displayable_plan = displayable(physical_plan.as_ref());

        println!("physical plan: {}", displayable_plan.indent());
        let result = collect(
            physical_plan,
            Arc::new(RuntimeEnv::new(RuntimeConfig::new())?),
        )
        .await?;
        let concatenated = concat_batches(&result[0].schema(), &result, 0)?;

        print_batches(&[concatenated])?;
        Ok(())
    }
}
