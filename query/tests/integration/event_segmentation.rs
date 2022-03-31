#[cfg(test)]
mod tests {
    use query::error::Result;
    use std::env::temp_dir;

    use chrono::{DateTime, Duration, Utc};
    use datafusion::arrow::array::{
        Float64Array, Int32Array, Int8Array, StringArray, TimestampMicrosecondArray, UInt16Array,
        UInt64Array,
    };

    use arrow::datatypes::{DataType as DFDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use datafusion::datasource::object_store::local::LocalFileSystem;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::{aggregates, collect, PhysicalPlanner};
    use datafusion::prelude::CsvReadOptions;

    use common::{DataType, ScalarValue, DECIMAL_PRECISION, DECIMAL_SCALE};
    use datafusion::execution::context::ExecutionContextState;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::logical_plan::{LogicalPlan, TableScan};
    use datafusion_expr::AggregateFunction;
    use metadata::database::{Column, Table, TableType};
    use metadata::properties::provider::Namespace;
    use metadata::properties::{CreatePropertyRequest, Property};
    use metadata::{database, events, properties, Metadata, Store};
    use query::common::{PropValueOperation, PropertyRef, QueryTime, TimeUnit};
    use query::event_segmentation::{
        Analysis, Breakdown, ChartType, Event, EventFilter, EventRef, EventSegmentation,
        LogicalPlanBuilder, NamedQuery, Query,
    };
    use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
    use query::{event_fields, Context};
    use rust_decimal::Decimal;
    use std::ops::Sub;
    use std::sync::Arc;
    use uuid::Uuid;

    async fn events_provider(
        db: Arc<database::Provider>,
        org_id: u64,
        proj_id: u64,
    ) -> Result<LogicalPlan> {
        let table = db.get_table(TableType::Events(org_id, proj_id)).await?;
        let schema = table.arrow_schema();
        let options = CsvReadOptions::new().schema(&schema);
        let path = "../tests/events.csv";
        let df_input = datafusion::logical_plan::LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            path,
            options,
            None,
            1,
        )
        .await?;

        Ok(df_input.build()?)
    }

    async fn create_property(
        md: &Arc<Metadata>,
        ns: Namespace,
        org_id: u64,
        proj_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let prop = match ns {
            Namespace::Event => md.event_properties.create(org_id, req).await?,
            Namespace::User => md.user_properties.create(org_id, req).await?,
        };

        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new(prop.column_name(ns), prop.typ.clone(), prop.nullable),
            )
            .await?;

        Ok(prop)
    }

    async fn create_entities(md: Arc<Metadata>, org_id: u64, proj_id: u64) -> Result<()> {
        md.database
            .create_table(Table {
                typ: TableType::Events(org_id, proj_id),
                columns: vec![],
            })
            .await?;

        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new(event_fields::USER_ID.to_string(), DFDataType::UInt64, false),
            )
            .await?;
        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new(
                    event_fields::CREATED_AT.to_string(),
                    DFDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    false,
                ),
            )
            .await?;
        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new(event_fields::EVENT.to_string(), DFDataType::UInt16, false),
            )
            .await?;

        // create user props
        create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                project_id: proj_id,
                tags: None,
                name: "Country".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                project_id: proj_id,
                tags: None,
                name: "Device".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                project_id: proj_id,
                tags: None,
                name: "Is Premium".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Boolean,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        // create events
        md.events
            .create(
                org_id,
                events::CreateEventRequest {
                    created_by: 0,
                    project_id: proj_id,
                    tags: None,
                    name: "View Product".to_string(),
                    display_name: None,
                    description: None,
                    status: events::Status::Enabled,
                    scope: events::Scope::User,
                    properties: None,
                    custom_properties: None,
                },
            )
            .await?;

        md.events
            .create(
                org_id,
                events::CreateEventRequest {
                    created_by: 0,
                    project_id: proj_id,
                    tags: None,
                    name: "Buy Product".to_string(),
                    display_name: None,
                    description: None,
                    status: events::Status::Enabled,
                    scope: events::Scope::User,
                    properties: None,
                    custom_properties: None,
                },
            )
            .await?;

        // create event props
        create_property(
            &md,
            Namespace::Event,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                project_id: proj_id,
                tags: None,
                name: "Product Name".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        create_property(
            &md,
            Namespace::Event,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                project_id: proj_id,
                tags: None,
                name: "Revenue".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Float64,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        Ok(())
    }

    fn create_md() -> Result<Arc<Metadata>> {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let store = Arc::new(Store::new(path));
        Ok(Arc::new(Metadata::try_new(store)?))
    }

    #[tokio::test]
    async fn test_filters() -> Result<()> {
        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
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
                    EventRef::Regular("View Product".to_string()),
                    Some(vec![EventFilter::Property {
                        property: PropertyRef::Event("Is Premium".to_string()),
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
                    EventRef::Regular("Buy Product".to_string()),
                    Some(vec![
                        EventFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::IsNull,
                            value: None,
                        },
                        EventFilter::Property {
                            property: PropertyRef::Event("Revenue".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::Number(Some(Decimal::new(1, 0))),
                                ScalarValue::Number(Some(Decimal::new(2, 0))),
                                ScalarValue::Number(Some(Decimal::new(3, 0))),
                            ]),
                        },
                        EventFilter::Property {
                            property: PropertyRef::User("Country".to_string()),
                            operation: PropValueOperation::IsNull,
                            value: None,
                        },
                        EventFilter::Property {
                            property: PropertyRef::User("Country".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![
                                ScalarValue::String(Some("Spain".to_string())),
                                ScalarValue::String(Some("France".to_string())),
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
                    value: Some(vec![ScalarValue::String(Some("Iphone".to_string()))]),
                },
                EventFilter::Property {
                    property: PropertyRef::User("Is Premium".to_string()),
                    operation: PropValueOperation::Eq,
                    value: Some(vec![ScalarValue::Number(Some(Decimal::new(1, 0)))]),
                },
            ]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Device".to_string(),
            ))]),
            segments: None,
        };

        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let store = Arc::new(Store::new(path));
        let md = Arc::new(Metadata::try_new(store)?);

        let org_id = 1;
        let proj_id = 1;

        let ctx = Context {
            organization_id: org_id,
            account_id: 1,
            project_id: proj_id,
            roles: None,
            permissions: None,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let input = Arc::new(events_provider(md.database.clone(), org_id, proj_id).await?);

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
        let to = DateTime::parse_from_rfc3339("2021-09-08T15:42:29.190855+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let es = EventSegmentation {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Regular("Buy Product".to_string()),
                None,
                None, //Some(vec![Breakdown::Property(PropertyRef::Event("Product Name".to_string()))]),
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
            )],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Country".to_string(),
            ))]),
            segments: None,
        };

        let md = create_md()?;

        let org_id = 1;
        let proj_id = 1;

        let ctx = Context {
            organization_id: org_id,
            account_id: 1,
            project_id: proj_id,
            roles: None,
            permissions: None,
        };

        create_entities(md.clone(), org_id, proj_id).await?;
        let input = Arc::new(events_provider(md.database.clone(), org_id, proj_id).await?);

        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es).await?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&plan, &ctx_state).await?;

        let result = collect(
            physical_plan,
            Arc::new(RuntimeEnv::new(RuntimeConfig::new())?),
        )
        .await?;

        print_batches(&result)?;
        Ok(())
    }
}
