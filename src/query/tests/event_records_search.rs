use std::sync::Arc;

use arrow::util::pretty::print_batches;
use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::query::QueryTime;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use metadata::test_util::init_db;
use query::physical_plan::planner::QueryPlanner;
use query::queries::event_records_search::EventRecordsSearch;
use query::queries::event_records_search::LogicalPlanBuilder;
use query::test_util::create_entities;
use query::test_util::events_provider;
use query::Context;

#[cfg(test)]
mod tests {
    use std::ops::Sub;
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use common::query::EventFilter;
    use common::query::EventRef;
    use common::query::PropValueOperation;
    use common::query::PropertyRef;
    use common::query::QueryTime;
    use common::query::TimeIntervalUnit;
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
    use query::queries::event_records_search::EventRecordsSearch;
    use query::queries::event_records_search::LogicalPlanBuilder;
    use query::queries::property_values::Filter;
    use query::queries::property_values::PropertyValues;
    use query::test_util::create_entities;
    use query::test_util::events_provider;
    use query::test_util::run_plan;
    use query::Context;

    #[tokio::test]
    async fn test_full() -> Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let req = EventRecordsSearch {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            events: None,
            filters: None,
            properties: None,
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        let result = run_plan(plan).await?;
        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_filter() -> query::Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let req = EventRecordsSearch {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            events: None,
            filters: Some(vec![EventFilter::Property {
                property: PropertyRef::Event("Revenue".to_string()),
                operation: PropValueOperation::Eq,
                value: Some(vec![ScalarValue::Decimal128(
                    Some(5335000000000000000),
                    DECIMAL_PRECISION,
                    DECIMAL_SCALE,
                )]),
            }]),
            properties: None,
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        let result = run_plan(plan).await?;
        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_str() -> query::Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let req = EventRecordsSearch {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            events: None,
            filters: Some(vec![EventFilter::Property {
                property: PropertyRef::User("Country".to_string()),
                operation: PropValueOperation::Like,
                value: Some(vec![ScalarValue::Utf8(Some("spa%".to_string()))]),
            }]),
            properties: None,
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        let result = run_plan(plan).await?;
        print_batches(&result)?;
        Ok(())
    }

    #[tokio::test]
    async fn test_projection() -> query::Result<()> {
        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            format: Default::default(),
            cur_time: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;

        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let req = EventRecordsSearch {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            events: None,
            filters: None,
            properties: Some(vec![PropertyRef::Event("Revenue".to_string())]),
        };

        let plan = LogicalPlanBuilder::build(ctx, md, input, req).await?;
        let result = run_plan(plan).await?;

        print_batches(&result)?;
        Ok(())
    }
}
