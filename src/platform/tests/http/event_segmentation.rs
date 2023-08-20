#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use chrono::DateTime;
    use chrono::Utc;
    use platform::queries::event_segmentation::Analysis;
    use platform::queries::event_segmentation::Breakdown;
    use platform::queries::event_segmentation::ChartType;
    use platform::queries::event_segmentation::Event;
    use platform::queries::event_segmentation::EventSegmentation;
    use platform::queries::event_segmentation::Query;
    use platform::queries::AggregateFunction;
    use platform::queries::PartitionedAggregateFunction;
    use platform::queries::QueryTime;
    use platform::queries::TimeIntervalUnit;
    use platform::EventFilter;
    use platform::EventRef;
    use platform::PropValueOperation;
    use platform::PropertyRef;
    use reqwest::Client;
    use serde_json::Value;
    use tracing::debug;

    use crate::assert_response_status_eq;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_event_segmentation() -> anyhow::Result<()> {
        let (base_url, md, pp) = run_http_service(true).await?;
        let cl = Client::new();
        let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

        let from =
            DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")?.with_timezone(&Utc);
        let to =
            DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")?.with_timezone(&Utc);

        let es = EventSegmentation {
            time: QueryTime::Between { from, to },
            group: "event_user_id".to_string(),
            interval_unit: TimeIntervalUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event {
                    event: EventRef::Regular {
                        event_name: "View Product".to_string(),
                    },
                    filters: Some(vec![EventFilter::Property {
                        property: PropertyRef::User {
                            property_name: "Is Premium".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![Value::Bool(true)]),
                    }]),
                    breakdowns: Some(vec![Breakdown::Property {
                        property: PropertyRef::User {
                            property_name: "Device".to_string(),
                        },
                    }]),
                    queries: vec![Query::CountEvents],
                },
                Event {
                    event: EventRef::Regular {
                        event_name: "Buy Product".to_string(),
                    },
                    filters: None,
                    breakdowns: None,
                    queries: vec![
                        Query::CountEvents,
                        Query::CountUniqueGroups,
                        Query::CountPerGroup {
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event {
                                property_name: "Revenue".to_string(),
                            },
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregateProperty {
                            property: PropertyRef::Event {
                                property_name: "Revenue".to_string(),
                            },
                            aggregate: AggregateFunction::Sum,
                        },
                    ],
                },
            ],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::User {
                    property_name: "Country".to_string(),
                },
            }]),
            segments: None,
        };

        let resp = cl
            .post(format!(
                "{base_url}/organizations/1/projects/1/queries/event-_segmentation"
            ))
            .body(serde_json::to_string(&es)?)
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_response_status_eq!(resp, StatusCode::OK);
        let txt = resp.text().await?;
        debug!("{}", &txt);

        Ok(())
    }
}
