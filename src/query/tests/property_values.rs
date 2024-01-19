#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use common::query::EventRef;
    use common::query::PropValueOperation;
    use common::query::PropertyRef;
    use common::DECIMAL_PRECISION;
    use common::DECIMAL_SCALE;
    use datafusion::execution::context::SessionState;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use datafusion_common::ScalarValue;
    use metadata::test_util::init_db;
    use query::error::Result;
    use query::physical_plan::planner::QueryPlanner;
    use query::queries::property_values::Filter;
    use query::queries::property_values::LogicalPlanBuilder;
    use query::queries::property_values::PropertyValues;
    use query::test_util::create_entities;
    use query::test_util::events_provider;
    use query::Context;

    #[tokio::test]
    async fn test_bool() -> Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::User("Is Premium".to_string()),
            event: Some(EventRef::RegularName("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::True,
                value: None,
            }),
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        println!("{:?}", plan);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        #[allow(deprecated)]
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);
        #[allow(deprecated)]
        let exec_ctx = SessionContext::with_state(session_state.clone());
        let physical_plan = session_state.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_decimal() -> Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::Event("Revenue".to_string()),
            event: Some(EventRef::RegularName("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::Eq,
                value: Some(vec![ScalarValue::Decimal128(
                    Some(5335000000000000000),
                    DECIMAL_PRECISION,
                    DECIMAL_SCALE,
                )]),
            }),
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        println!("{:?}", plan);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        #[allow(deprecated)]
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);
        #[allow(deprecated)]
        let exec_ctx = SessionContext::with_state(session_state.clone());
        let physical_plan = session_state.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_str() -> Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::Event("Product Name".to_string()),
            event: Some(EventRef::RegularName("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::Like,
                value: Some(vec![ScalarValue::Utf8(Some("goo%".to_string()))]),
            }),
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        println!("{:?}", plan);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        #[allow(deprecated)]
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);
        #[allow(deprecated)]
        let exec_ctx = SessionContext::with_state(session_state.clone());
        let physical_plan = session_state.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_str_dict() -> Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let req = PropertyValues {
            property: PropertyRef::User("Country".to_string()),
            event: Some(EventRef::RegularName("View Product".to_string())),
            filter: Some(Filter {
                operation: PropValueOperation::Like,
                value: Some(vec![ScalarValue::Utf8(Some("spa%".to_string()))]),
            }),
        };
        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        println!("{:?}", plan);
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        #[allow(deprecated)]
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);
        #[allow(deprecated)]
        let exec_ctx = SessionContext::with_state(session_state.clone());
        let physical_plan = session_state.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        print_batches(&result)?;
        Ok(())
    }
}
