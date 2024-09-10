#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use common::GROUP_USER_ID;
    use platform::event_segmentation::Analysis;
    use platform::event_segmentation::ChartType;
    use platform::event_segmentation::Event;
    use platform::event_segmentation::EventSegmentationRequest;
    use platform::event_segmentation::Query;
    use platform::AggregateFunction;
    use platform::Breakdown;
    use platform::EventRef;
    use platform::PartitionedAggregateFunction;
    use platform::PropValueFilter;
    use platform::PropValueOperation;
    use platform::PropertyRef;
    use platform::QueryTime;
    use platform::TimeIntervalUnit;
    use reqwest::Client;
    use reqwest::StatusCode;
    use serde_json::Value;
    use tracing::debug;

    use crate::assert_response_status_eq;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_event_segmentation() {
        let (base_url, md, pp) = run_http_service(true).await.unwrap();
        let cl = Client::new();
        let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
            .await
            .unwrap();

        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let es = EventSegmentationRequest {
            time: QueryTime::Between { from, to },
            group: GROUP_USER_ID,
            interval_unit: TimeIntervalUnit::Hour,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event {
                    event: EventRef::Regular {
                        event_name: "View Product".to_string(),
                    },
                    filters: Some(vec![PropValueFilter::Property {
                        property: PropertyRef::Group {
                            property_name: "Is Premium".to_string(),
                            group: 0,
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![Value::Bool(true)]),
                    }]),
                    breakdowns: Some(vec![Breakdown::Property {
                        property: PropertyRef::Group {
                            property_name: "Device".to_string(),
                            group: 0,
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
                property: PropertyRef::Group {
                    property_name: "Country".to_string(),
                    group: 0,
                },
            }]),
            segments: None,
        };

        let resp = cl
            .post(format!("{base_url}/projects/1/queries/event-segmentation"))
            .body(serde_json::to_string(&es).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        let txt = resp.text().await.unwrap();
        debug!("{}", &txt);
    }
}
