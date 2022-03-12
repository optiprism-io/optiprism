#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use query::error::Result;
    use query::logical_plan::expr::Expr;

    use query::logical_plan::plan::LogicalPlan;
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
    use datafusion::execution::context::ExecutionContextState;
    use datafusion::logical_plan::LogicalPlan as DFLogicalPlan;
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::{aggregates, collect, PhysicalPlanner};
    use datafusion::prelude::CsvReadOptions;

    use std::ops::Sub;
    use std::sync::Arc;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::logical_plan::TableScan;
    use rust_decimal::Decimal;
    use uuid::Uuid;
    use common::{DataType, ScalarValue};
    use metadata::{events, Metadata, properties, Store};
    use metadata::properties::CreatePropertyRequest;
    use query::Context;
    use query::event_segmentation::{LogicalPlanBuilder, Analysis, Breakdown, ChartType, Event, event_fields, EventFilter, EventRef, EventSegmentation, NamedQuery, Operation, PropertyRef, Query, QueryTime, TimeUnit};
    use query::physical_plan::expressions::aggregate::AggregateFunction;
    use query::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;

    fn users_provider() -> Result<MemTable> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "created_at",
                DFDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("b", DFDataType::Int32, false),
            Field::new("c", DFDataType::Int32, false),
            Field::new("d", DFDataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        Ok(MemTable::try_new(schema, vec![vec![batch]])?)
    }

    fn events_schema() -> Schema {
        Schema::new(vec![
            Field::new(event_fields::USER_ID, DFDataType::UInt64, false),
            Field::new(
                event_fields::CREATED_AT,
                DFDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(event_fields::EVENT, DFDataType::UInt16, false),
            Field::new("country", DFDataType::Utf8, true),
            Field::new("device", DFDataType::Utf8, true),
            Field::new("0_float64", DFDataType::Float64, true),
            Field::new("1_utf8", DFDataType::Utf8, true),
            Field::new("2_int8", DFDataType::Int8, true),
        ])
    }

    async fn events_provider() -> Result<LogicalPlan> {
        let schema = Arc::new(events_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![0u64; 0])),
                Arc::new(TimestampMicrosecondArray::from(vec![0i64; 0])),
                Arc::new(UInt16Array::from(vec![0u16; 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(Float64Array::from(vec![0f64; 0])),
                Arc::new(StringArray::from(vec!["".to_string(); 0])),
                Arc::new(Int8Array::from(vec![0i8; 0])),
            ],
        )?;

        let _prov = MemTable::try_new(schema.clone(), vec![vec![batch]])?;
        let path = "../tests/events.csv";

        let schema = events_schema();
        let options = CsvReadOptions::new().schema(&schema);
        let df_input =
            datafusion::logical_plan::LogicalPlanBuilder::scan_csv(Arc::new(LocalFileSystem {}), path, options, None, 1)
                .await?;

        Ok(match df_input.build()? {
            DFLogicalPlan::TableScan(t) => LogicalPlan::TableScan {
                table_name: t.table_name,
                source: t.source,
                projection: t.projection,
                projected_schema: t.projected_schema,
                filters: t.filters.iter().map(Expr::from_df_expr).collect::<Result<_>>()?,
                limit: t.limit,
            },
            _ => unreachable!(),
        })
    }

    async fn create_entities(metadata: Arc<Metadata>, org_id: u64, proj_id: u64) -> Result<()> {
        // create events
        metadata.events.create(org_id, events::CreateEventRequest {
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
        }).await?;

        metadata.events.create(org_id, events::CreateEventRequest {
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
        }).await?;

        // create event props
        metadata.event_properties.create(org_id, CreatePropertyRequest {
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
        }).await?;

        metadata.event_properties.create(org_id, CreatePropertyRequest {
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
        }).await?;

        // create user props
        metadata.user_properties.create(org_id, CreatePropertyRequest {
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
        }).await?;

        metadata.user_properties.create(org_id, CreatePropertyRequest {
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
        }).await?;

        metadata.user_properties.create(org_id, CreatePropertyRequest {
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
        }).await?;

        Ok(())
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
            interval_unit: TimeUnit::Day,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![Event::new(
                EventRef::Regular("Buy Product".to_string()),
                Some(vec![
                    EventFilter::Property {
                        property: PropertyRef::Event("revenue".to_string()),
                        operation: Operation::IsNull,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("revenue".to_string()),
                        operation: Operation::Eq,
                        value: Some(vec![
                            ScalarValue::Number(Some(Decimal::new(1, 0))),
                            ScalarValue::Number(Some(Decimal::new(2, 0))),
                            ScalarValue::Number(Some(Decimal::new(3, 0))),
                        ]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::User("country".to_string()),
                        operation: Operation::IsNull,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::User("country".to_string()),
                        operation: Operation::Eq,
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
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: PartitionedAggregateFunction::Avg,
                            aggregate: AggregateFunction::Avg,
                        },
                        Some("avg_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: PartitionedAggregateFunction::Min,
                            aggregate: AggregateFunction::Avg,
                        },
                        Some("min_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregateProperty {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate: AggregateFunction::Sum,
                        },
                        Some("sum_revenue".to_string()),
                    ),
                ],
            )],
            filters: Some(vec![
                EventFilter::Property {
                    property: PropertyRef::User("device".to_string()),
                    operation: Operation::Eq,
                    value: Some(vec![ScalarValue::String(Some("Iphone".to_string()))]),
                },
                EventFilter::Property {
                    property: PropertyRef::User("is_premium".to_string()),
                    operation: Operation::Eq,
                    value: Some(vec![ScalarValue::Number(Some(Decimal::new(1, 0)))]),
                },
            ]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "device".to_string(),
            ))]),
            segments: None,
        };

        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let store = Arc::new(Store::new(path));
        let md = Arc::new(Metadata::try_new(store)?);
        create_entities(md.clone(), 1, 1).await?;

        let ctx = Context {
            organization_id: 1,
            account_id: 1,
            project_id: 1,
            roles: None,
            permissions: None,
        };

        let input = Arc::new(events_provider().await?);

        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es).await?;
        let df_plan = plan.to_df_plan()?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;

        let result = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new()).unwrap())).await?;

        print_batches(&result)?;
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
            interval_unit: TimeUnit::Day,
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
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Some("avg_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregateProperty {
                            property: PropertyRef::Event("revenue".to_string()),
                            aggregate: AggregateFunction::Sum,
                        },
                        Some("sum_revenue".to_string()),
                    ),
                ],
            )],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "country".to_string(),
            ))]),
            segments: None,
        };

        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let store = Arc::new(Store::new(path));
        let md = Arc::new(Metadata::try_new(store)?);
        create_entities(md.clone(), 1, 1).await?;

        let ctx = Context {
            organization_id: 1,
            account_id: 1,
            project_id: 1,
            roles: None,
            permissions: None,
        };

        let input = Arc::new(events_provider().await?);

        let plan = LogicalPlanBuilder::build(ctx, md.clone(), input, es).await?;
        let df_plan = plan.to_df_plan()?;

        let mut ctx_state = ExecutionContextState::new();
        ctx_state.config.target_partitions = 1;
        let planner = DefaultPhysicalPlanner::default();
        let physical_plan = planner.create_physical_plan(&df_plan, &ctx_state).await?;

        let result = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new())?)).await?;

        print_batches(&result)?;
        Ok(())
    }
}