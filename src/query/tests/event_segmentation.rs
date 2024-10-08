#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::ops::Sub;
    use std::sync::Arc;

    use arrow::util::pretty::print_batches;
    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use common::event_segmentation::Analysis;
    use common::event_segmentation::ChartType;
    use common::event_segmentation::Event;
    use common::event_segmentation::EventSegmentationRequest;
    use common::event_segmentation::NamedQuery;
    use common::event_segmentation::Query;
    use common::query::AggregateFunction;
    use common::query::Breakdown;
    use common::query::EventRef;
    use common::query::PartitionedAggregateFunction;
    use common::query::PropValueFilter;
    use common::query::PropValueOperation;
    use common::query::PropertyRef;
    use common::query::QueryTime;
    use common::query::TimeIntervalUnit;
    use common::GROUP_USER_ID;
    use datafusion_common::ScalarValue;
    use metadata::custom_events;
    use metadata::custom_events::CreateCustomEventRequest;
    use metadata::util::init_db;
    use query::error::Result;
    use query::event_segmentation::LogicalPlanBuilder;
    use query::test_util::create_entities;
    use query::test_util::events_provider;
    use query::test_util::run_plan;
    use query::Context;
    use tracing_test::traced_test;
    use uuid::Uuid;

    #[traced_test]
    #[tokio::test]
    async fn test_filters() -> Result<()> {
        let to = DateTime::parse_from_rfc3339("2022-08-30T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _es = EventSegmentationRequest {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            group_id: GROUP_USER_ID,
            interval_unit: TimeIntervalUnit::Hour,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event::new(
                    EventRef::RegularName("View Product".to_string()),
                    Some(vec![PropValueFilter::Property {
                        property: PropertyRef::Group("Is Premium".to_string(), 0),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Boolean(Some(true))]),
                    }]),
                    Some(vec![Breakdown::Property(PropertyRef::Group(
                        "Device".to_string(),
                        0,
                    ))]),
                    vec![NamedQuery::new(
                        Query::CountEvents,
                        Some("count".to_string()),
                    )],
                ),
                Event::new(
                    EventRef::RegularName("Buy Product".to_string()),
                    Some(vec![
                        PropValueFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::Empty,
                            value: None,
                        },
                        PropValueFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::Decimal128(Some(1), 1, 0),
                                ScalarValue::Decimal128(Some(2), 1, 0),
                                ScalarValue::Decimal128(Some(3), 1, 0),
                            ]),
                        },
                        PropValueFilter::Property {
                            property: PropertyRef::Group("Country".to_string(), 0),
                            operation: PropValueOperation::Empty,
                            value: None,
                        },
                        PropValueFilter::Property {
                            property: PropertyRef::Group("Country".to_string(), 0),
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
                PropValueFilter::Property {
                    property: PropertyRef::Group("Device".to_string(), 0),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Utf8(Some("Iphone".to_string()))]),
                },
                PropValueFilter::Property {
                    property: PropertyRef::Group("Is Premium".to_string(), 0),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Decimal128(Some(1), 1, 0)]),
                },
            ]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::Group(
                "Device".to_string(),
                0,
            ))]),
            segments: None,
        };

        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        let (md, db) = init_db()?;

        let proj_id = 1;

        let _ctx = Context {
            project_id: proj_id,
            cur_time: Default::default(),
            format: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let _input = Arc::new(events_provider(db, proj_id).await?);

        // let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es).await?;
        // let df_plan = plan.to_df_plan()?;
        //
        // let mut ctx_state = ExecutionContextState::new();
        // ctx_state.config.target_partitions = 1;
        // let planner = DefaultPhysicalPlanner::default();
        // let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;
        //
        // let result = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new()).unwrap())).await?;
        //
        // print_batches(&result)?;
        Ok(())
    }

    #[traced_test]
    #[tokio::test]
    async fn test_query() -> Result<()> {
        let _from = DateTime::parse_from_rfc3339("2020-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentationRequest {
            // time: QueryTime::Between {
            // from,
            // to,
            // },
            time: QueryTime::Last {
                last: 20,
                unit: TimeIntervalUnit::Day,
            },
            group_id: GROUP_USER_ID,
            interval_unit: TimeIntervalUnit::Day,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                /* Event::new(
                     EventRef::RegularName("View Product".to_string()),
                     // Some(vec![
                     // EventFilter::Property {
                     // property: PropertyRef::User("Is Premium".to_string()),
                     // operation: PropValueOperation::Eq,
                     // value: Some(vec![ScalarValue::Boolean(Some(true))]),
                     // },
                     // EventFilter::Property {
                     // property: PropertyRef::User("Country".to_string()),
                     // operation: PropValueOperation::Eq,
                     // value: Some(vec![
                     // ScalarValue::Utf8(Some("spain".to_string())),
                     // ScalarValue::Utf8(Some("german".to_string())),
                     // ]),
                     // },
                     // ]),
                     None,
                     Some(vec![Breakdown::Property(PropertyRef::User(
                         "Device".to_string(),
                     ))]),
                     // None,
                     vec![NamedQuery::new(
                         Query::CountEvents,
                         Some("0_count".to_string()),
                     )],
                 ),*/
                Event::new(
                    EventRef::RegularName("Buy Product".to_string()),
                    None,
                    None, /* Some(vec![Breakdown::Property(PropertyRef::Event("Product Name".to_string()))]), */
                    vec![
                        NamedQuery::new(Query::CountEvents, Some("0_count".to_string())),
                        NamedQuery::new(Query::CountUniqueGroups, Some("1_count".to_string())),
                        NamedQuery::new(
                            Query::CountPerGroup {
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("2_count".to_string()),
                        ),
                        NamedQuery::new(
                            Query::AggregatePropertyPerGroup {
                                property: PropertyRef::Event("Revenue".to_string()),
                                aggregate_per_group: PartitionedAggregateFunction::Sum,
                                aggregate: AggregateFunction::Avg,
                            },
                            Some("3_agg".to_string()),
                        ),
                        // NamedQuery::new(
                        // Query::AggregateProperty {
                        // property: PropertyRef::Event("Revenue".to_string()),
                        // aggregate: AggregateFunction::Sum,
                        // },
                        // Some("4_agg".to_string()),
                        // ),
                    ],
                ),
            ],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::Group(
                "Country".to_string(),
                0,
            ))]),
            // breakdowns: None,
            segments: None,
        };

        let (md, db) = init_db()?;

        let proj_id = 1;

        let ctx = Context {
            project_id: proj_id,
            cur_time: Default::default(),
            format: Default::default(),
        };

        create_entities(md.clone(), &db, proj_id).await?;
        let input = events_provider(db, proj_id).await?;
        let _cur_time = DateTime::parse_from_rfc3339("2021-09-16T13:49:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es)?;
        let result = run_plan(plan).await?;
        print_batches(&result)?;

        Ok(())
    }

    #[traced_test]
    #[tokio::test]
    async fn test_custom_events() -> Result<()> {
        let _org_id = 1;
        let proj_id = 1;
        let (md, db) = init_db()?;
        create_entities(md.clone(), &db, proj_id).await?;

        let custom_event = md
            .custom_events
            .create(1, CreateCustomEventRequest {
                created_by: 0,
                tags: None,
                name: "".to_string(),
                description: None,
                status: custom_events::Status::Enabled,
                is_system: false,
                events: vec![
                    custom_events::Event {
                        event: EventRef::RegularName("View Product".to_string()),
                        filters: Some(vec![
                            PropValueFilter::Property {
                                property: PropertyRef::Group("Is Premium".to_string(), 0),
                                operation: PropValueOperation::Eq,
                                value: Some(vec![ScalarValue::Boolean(Some(true))]),
                            },
                            PropValueFilter::Property {
                                property: PropertyRef::Group("Country".to_string(), 0),
                                operation: PropValueOperation::Eq,
                                value: Some(vec![
                                    ScalarValue::Utf8(Some("spain".to_string())),
                                    ScalarValue::Utf8(Some("german".to_string())),
                                ]),
                            },
                        ]),
                    },
                    custom_events::Event {
                        event: EventRef::RegularName("Buy Product".to_string()),
                        filters: None,
                    },
                ],
            })
            .unwrap();

        let _from = DateTime::parse_from_rfc3339("2020-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentationRequest {
            time: QueryTime::Last {
                last: 30,
                unit: TimeIntervalUnit::Day,
            },
            group_id: GROUP_USER_ID,
            interval_unit: TimeIntervalUnit::Week,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Custom(custom_event.id),
                None,
                Some(vec![Breakdown::Property(PropertyRef::Group(
                    "Device".to_string(),
                    0,
                ))]),
                vec![NamedQuery::new(
                    Query::CountEvents,
                    Some("count1".to_string()),
                )],
            )],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::Group(
                "Country".to_string(),
                0,
            ))]),
            segments: None,
        };

        let ctx = Context {
            project_id: proj_id,
            cur_time: Default::default(),
            format: Default::default(),
        };

        let input = events_provider(db, proj_id).await?;
        let _cur_time = DateTime::parse_from_rfc3339("2022-08-29T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es)?;
        let result = run_plan(plan).await?;
        print_batches(&result)?;

        Ok(())
    }
}
