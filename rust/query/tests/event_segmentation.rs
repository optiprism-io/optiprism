#[cfg(test)]
mod tests {
    use query::error::Result;

    use std::env::temp_dir;

    use chrono::{DateTime, Duration, Utc};

    use arrow::util::pretty::print_batches;

    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use datafusion::execution::runtime_env::RuntimeEnv;

    use datafusion::physical_plan::coalesce_batches::concat_batches;

    use datafusion_expr::AggregateFunction;

    use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
    use query::physical_plan::planner::QueryPlanner;
    use query::queries::event_segmentation::logical_plan_builder::LogicalPlanBuilder;
    use query::queries::event_segmentation::types::{
        Analysis, Breakdown, ChartType, Event, EventSegmentation, NamedQuery, Query,
    };
    use query::queries::types::{QueryTime, TimeUnit};
    use query::{event_fields, Context};

    use common::types::{EventFilter, EventRef, PropValueOperation, PropertyRef};
    use common::ScalarValue;
    use datafusion::execution::context::SessionState;
    use metadata::custom_events;
    use metadata::custom_events::CreateCustomEventRequest;
    use query::test_util::{create_entities, create_md, events_provider};
    use std::ops::Sub;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_filters() -> Result<()> {
        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _es = EventSegmentation {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event::new(
                    EventRef::RegularName("View Product".to_string()),
                    Some(vec![EventFilter::Property {
                        property: PropertyRef::User("Is Premium".to_string()),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Boolean(Some(true))]),
                    }]),
                    Some(vec![Breakdown::Property(PropertyRef::User(
                        "Device".to_string(),
                    ))]),
                    vec![NamedQuery::new(
                        Query::CountEvents,
                        Some("count".to_string()),
                    )],
                ),
                Event::new(
                    EventRef::RegularName("Buy Product".to_string()),
                    Some(vec![
                        EventFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::Empty,
                            value: None,
                        },
                        EventFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::Decimal128(Some(1), 1, 0),
                                ScalarValue::Decimal128(Some(2), 1, 0),
                                ScalarValue::Decimal128(Some(3), 1, 0),
                            ]),
                        },
                        EventFilter::Property {
                            property: PropertyRef::User("Country".to_string()),
                            operation: PropValueOperation::Empty,
                            value: None,
                        },
                        EventFilter::Property {
                            property: PropertyRef::User("Country".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::Utf8(Some("Spain".to_string())),
                                ScalarValue::Utf8(Some("France".to_string())),
                            ]),
                        },
                    ]),
                    Some(vec![Breakdown::Property(PropertyRef::Event(
                        "Product Name".to_string(),
                    ))]),
                    vec![
                        NamedQuery::new(Query::CountEvents, Some("count".to_string())),
                        NamedQuery::new(
                            Query::CountUniqueGroups,
                            Some("count_unique_users".to_string()),
                        ),
                        NamedQuery::new(
                            Query::CountPerGroup {
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("count_per_user".to_string()),
                        ),
                        NamedQuery::new(
                            Query::AggregatePropertyPerGroup {
                                property: PropertyRef::Event("Revenue".to_string()),
                                aggregate_per_group: PartitionedAggregateFunction::Sum,
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("avg_total_revenue_per_user".to_string()),
                        ),
                        NamedQuery::new(
                            Query::AggregateProperty {
                                property: PropertyRef::Event("Revenue".to_string()),
                                aggregate: AggregateFunction::Sum,
                            },
                            Some("sum_revenue".to_string()),
                        ),
                    ],
                ),
            ],
            filters: Some(vec![
                EventFilter::Property {
                    property: PropertyRef::User("Device".to_string()),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Utf8(Some("Iphone".to_string()))]),
                },
                EventFilter::Property {
                    property: PropertyRef::User("Is Premium".to_string()),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Decimal128(Some(1), 1, 0)]),
                },
            ]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Device".to_string(),
            ))]),
        };

        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let md = create_md()?;

        let org_id = 1;
        let proj_id = 1;

        let _ctx = Context {
            organization_id: org_id,
            project_id: proj_id,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let _input = Arc::new(events_provider(md.database.clone(), org_id, proj_id).await?);

        /*let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es).await?;
        let df_plan = plan.to_df_plan()?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;

        let result = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new()).unwrap())).await?;

        print_batches(&result)?;*/
        Ok(())
    }

    #[tokio::test]
    async fn test_query() -> Result<()> {
        let _from = DateTime::parse_from_rfc3339("2020-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            /*time: QueryTime::Between {
                from,
                to,
            },*/
            time: QueryTime::Last {
                last: 30,
                unit: TimeUnit::Day,
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Week,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event::new(
                    EventRef::RegularName("View Product".to_string()),
                    Some(vec![
                        EventFilter::Property {
                            property: PropertyRef::User("Is Premium".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![ScalarValue::Boolean(Some(true))]),
                        },
                        EventFilter::Property {
                            property: PropertyRef::User("Country".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::Utf8(Some("spain".to_string())),
                                ScalarValue::Utf8(Some("german".to_string())),
                            ]),
                        },
                    ]),
                    Some(vec![Breakdown::Property(PropertyRef::User(
                        "Device".to_string(),
                    ))]),
                    vec![NamedQuery::new(
                        Query::CountEvents,
                        Some("count1".to_string()),
                    )],
                ),
                Event::new(
                    EventRef::RegularName("Buy Product".to_string()),
                    None,
                    None, //Some(vec![Breakdown::Property(PropertyRef::Event("Product Name".to_string()))]),
                    vec![
                        NamedQuery::new(Query::CountEvents, Some("count2".to_string())),
                        NamedQuery::new(
                            Query::CountUniqueGroups,
                            Some("count_unique_users".to_string()),
                        ),
                        NamedQuery::new(
                            Query::CountPerGroup {
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("count_per_user".to_string()),
                        ),
                        NamedQuery::new(
                            Query::AggregatePropertyPerGroup {
                                property: PropertyRef::Event("Revenue".to_string()),
                                aggregate_per_group: PartitionedAggregateFunction::Sum,
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("avg_revenue_per_user".to_string()),
                        ),
                        NamedQuery::new(
                            Query::AggregateProperty {
                                property: PropertyRef::Event("Revenue".to_string()),
                                aggregate: AggregateFunction::Sum,
                            },
                            Some("sum_revenue".to_string()),
                        ),
                    ],
                ),
            ],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Country".to_string(),
            ))]),
        };

        let md = create_md()?;

        let org_id = 1;
        let proj_id = 1;

        let ctx = Context {
            organization_id: org_id,
            project_id: proj_id,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let input = events_provider(md.database.clone(), org_id, proj_id).await?;
        let cur_time = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let plan = LogicalPlanBuilder::build(ctx, cur_time, md.clone(), input, es).await?;
        println!("logical plan: {:?}", plan);

        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);

        let exec_ctx = SessionContext::with_state(session_state);
        let physical_plan = exec_ctx.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        let concated = concat_batches(&result[0].schema(), &result, 0)?;

        print_batches(&[concated])?;
        Ok(())
    }

    #[tokio::test]
    async fn test_custom_events() -> Result<()> {
        let org_id = 1;
        let proj_id = 1;
        let md = create_md()?;
        create_entities(md.clone(), org_id, proj_id).await?;

        let custom_event = md
            .custom_events
            .create(
                1,
                1,
                CreateCustomEventRequest {
                    created_by: 0,
                    tags: None,
                    name: "".to_string(),
                    description: None,
                    status: custom_events::Status::Enabled,
                    is_system: false,
                    events: vec![
                        custom_events::types::Event {
                            event: EventRef::RegularName("View Product".to_string()),
                            filters: Some(vec![
                                EventFilter::Property {
                                    property: PropertyRef::User("Is Premium".to_string()),
                                    operation: PropValueOperation::Eq,
                                    value: Some(vec![ScalarValue::Boolean(Some(true))]),
                                },
                                EventFilter::Property {
                                    property: PropertyRef::User("Country".to_string()),
                                    operation: PropValueOperation::Eq,
                                    value: Some(vec![
                                        ScalarValue::Utf8(Some("spain".to_string())),
                                        ScalarValue::Utf8(Some("german".to_string())),
                                    ]),
                                },
                            ]),
                        },
                        custom_events::types::Event {
                            event: EventRef::RegularName("Buy Product".to_string()),
                            filters: None,
                        },
                    ],
                },
            )
            .await
            .unwrap();

        let _from = DateTime::parse_from_rfc3339("2020-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Last {
                last: 30,
                unit: TimeUnit::Day,
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Week,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Custom(custom_event.id),
                None,
                Some(vec![Breakdown::Property(PropertyRef::User(
                    "Device".to_string(),
                ))]),
                vec![NamedQuery::new(
                    Query::CountEvents,
                    Some("count1".to_string()),
                )],
            )],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Country".to_string(),
            ))]),
        };

        let ctx = Context {
            organization_id: org_id,
            project_id: proj_id,
        };

        let input = events_provider(md.database.clone(), org_id, proj_id).await?;
        let cur_time = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let plan = LogicalPlanBuilder::build(ctx, cur_time, md.clone(), input, es).await?;
        println!("logical plan: {:?}", plan);

        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new().with_target_partitions(1);
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![]);

        let exec_ctx = SessionContext::with_state(session_state);
        let physical_plan = exec_ctx.create_physical_plan(&plan).await?;

        let result = collect(physical_plan, exec_ctx.task_ctx()).await?;

        let concated = concat_batches(&result[0].schema(), &result, 0)?;

        print_batches(&[concated])?;
        Ok(())
    }
}
