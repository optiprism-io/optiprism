#[cfg(test)]
mod tests {
    use std::ops::Sub;

    use chrono::DateTime;
    use chrono::Duration;
    use chrono::Utc;
    use common::query::funnel::ChartType;
    use common::query::funnel::Count;
    use common::query::funnel::Event;
    use common::query::funnel::Exclude;
    use common::query::funnel::Filter;
    use common::query::funnel::Funnel;
    use common::query::funnel::Step;
    use common::query::funnel::StepOrder;
    use common::query::funnel::TimeIntervalUnitSession;
    use common::query::funnel::TimeWindow;
    use common::query::funnel::Touch;
    use common::query::Breakdown;
    use common::query::EventFilter;
    use common::query::EventRef;
    use common::query::PropValueOperation;
    use common::query::PropertyRef;
    use common::query::QueryTime;
    use datafusion_common::ScalarValue;
    use metadata::util::init_db;
    use query::test_util::create_entities;
    use query::test_util::events_provider;
    use query::Context;

    #[test]
    async fn test_full() {
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

        let req = Funnel {
            time: QueryTime::Between {
                from: to.sub(Duration::days(10)),
                to,
            },
            group: "group".to_string(),
            steps: vec![
                Step {
                    events: vec![Event {
                        event: EventRef::RegularName("View Product".to_string()),
                        filters: Some(vec![EventFilter::Property {
                            property: PropertyRef::User("Is Premium".to_string()),
                            operation: PropValueOperation::Eq,
                            value: Some(vec![ScalarValue::Boolean(Some(true))]),
                        }]),
                    }],
                    order: StepOrder::Sequential,
                },
                Step {
                    events: vec![Event {
                        event: EventRef::RegularName("Buy Product".to_string()),
                        filters: None,
                    }],
                    order: StepOrder::Sequential,
                },
            ],
            time_window: TimeWindow {
                n: 1,
                unit: TimeIntervalUnitSession::Day,
            },
            chart_type: ChartType::Steps,
            count: Count::Unique,
            filter: Some(Filter::DropOffOnAnyStep),
            touch: Touch::First,
            step_order: StepOrder::Sequential,
            attribution: Some(Touch::First),
            holding_constants: Some(vec![PropertyRef::User("Is Premium".to_string())]),
            exclude: Some(vec![Exclude {
                event: Event {
                    event: EventRef::RegularName("Leave Site".to_string()),
                    filters: None,
                },
                steps: None,
            }]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Device".to_string(),
            ))]),
            segments: None,
            filters: Some(vec![EventFilter::Property {
                property: PropertyRef::User("Is Premium".to_string()),
                operation: PropValueOperation::Eq,
                value: Some(vec![ScalarValue::Boolean(Some(true))]),
            }]),
        };
    }
}
