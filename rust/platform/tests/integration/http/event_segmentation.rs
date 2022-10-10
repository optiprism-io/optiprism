#[cfg(test)]
mod tests {
    use axum::{Router, Server};

    use platform::error::Result;
    use platform::http::queries;
    use query::{Context, QueryProvider};

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use chrono::{DateTime, Utc};

    use axum::headers::{HeaderMap, HeaderValue};
    use axum::http::StatusCode;

    use platform::queries::event_segmentation::{
        Analysis, Breakdown, ChartType, Event, EventSegmentation, Query,
    };
    use platform::queries::types::{
        AggregateFunction, EventFilter, EventRef, PartitionedAggregateFunction, PropValueOperation,
        PropertyRef, QueryTime, TimeUnit,
    };
    use query::test_util::{create_entities, create_md, events_provider};
    use reqwest::Client;
    use serde_json::Value;
    use crate::http::tests::{create_admin_acc_and_login, run_http_service};

    #[tokio::test]
    async fn test_event_segmentation() -> Result<()> {
        let (base_url, md, pp) = run_http_service(true).await?;
        let cl = Client::new();
        let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts, &cl).await?;

        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let es = EventSegmentation {
            time: QueryTime::Between { from, to },
            group: "event_user_id".to_string(),
            interval_unit: TimeUnit::Minute,
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

        let body = serde_json::to_string(&es).unwrap();

        let resp = cl
            .post(format!("{base_url}/v1/organizations/1/projects/1/queries/event-segmentation"))
            .body(body)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        let status = resp.status();
        let txt = resp.text().await.unwrap();
        println!("{}", &txt);
        assert_eq!(status, StatusCode::OK);

        Ok(())
    }
}
