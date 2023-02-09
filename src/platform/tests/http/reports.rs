use chrono::Utc;
use common::types::OptionalProperty;
use platform::queries::event_segmentation::Analysis;
use platform::queries::event_segmentation::ChartType;
use platform::queries::event_segmentation::EventSegmentation;
use platform::queries::QueryTime;
use platform::queries::TimeIntervalUnit;
use platform::reports::CreateReportRequest;
use platform::reports::Query;
use platform::reports::Report;
use platform::reports::Type;
use platform::reports::UpdateReportRequest;
use platform::ListResponse;
use reqwest::Client;
use reqwest::StatusCode;

use crate::assert_response_json_eq;
use crate::assert_response_status_eq;
use crate::http::tests::create_admin_acc_and_login;
use crate::http::tests::run_http_service;
use crate::http::tests::EMPTY_LIST;

fn assert(l: &Report, r: &Report) {
    assert_eq!(l.id, 1);
    assert_eq!(l.project_id, r.project_id);
    assert_eq!(l.tags, r.tags);
    assert_eq!(l.name, r.name);
    assert_eq!(l.description, r.description);
    assert_eq!(l.query, r.query);
}

#[tokio::test]
async fn test_reports() -> anyhow::Result<()> {
    let (base_url, md, pp) = run_http_service(false).await?;
    let report_url = format!("{base_url}/organizations/1/projects/1/reports");
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

    let mut report = Report {
        id: 0,
        created_at: Default::default(),
        updated_at: None,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        tags: Some(vec!["sdf".to_string()]),
        name: "qwe".to_string(),
        description: Some("desc".to_string()),
        typ: Type::EventSegmentation,
        query: Query::EventSegmentation(EventSegmentation {
            time: QueryTime::From { from: Utc::now() },
            group: "user".to_string(),
            interval_unit: TimeIntervalUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![],
            filters: None,
            breakdowns: None,
            segments: None,
        }),
    };

    // list without reports should be empty
    {
        let resp = cl
            .get(format!("{report_url}/"))
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST.to_string());
    }

    // get of un-existing report 1 should return 404 not found error
    {
        let resp = cl
            .get(format!("{report_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // delete of un-existing report 1 should return 404 not found error
    {
        let resp = cl
            .delete(format!("{report_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // create request should create report
    {
        let req = CreateReportRequest {
            tags: report.tags.clone(),
            name: report.name.clone(),
            description: report.description.clone(),
            typ: Type::EventSegmentation,
            query: report.query.clone(),
        };

        let resp = cl
            .post(&report_url)
            .body(serde_json::to_string(&req)?)
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::CREATED);
        let resp: Report = serde_json::from_str(resp.text().await?.as_str())?;
        assert(&resp, &report)
    }

    // update request should update report
    {
        report.tags = Some(vec!["ert".to_string()]);
        report.description = Some("xcv".to_string());
        report.query = Query::EventSegmentation(EventSegmentation {
            time: QueryTime::Last {
                last: 1,
                unit: TimeIntervalUnit::Second,
            },
            group: "some".to_string(),
            interval_unit: TimeIntervalUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![],
            filters: None,
            breakdowns: None,
            segments: None,
        });

        let req = UpdateReportRequest {
            tags: OptionalProperty::Some(report.tags.clone()),
            name: OptionalProperty::Some(report.name.clone()),
            description: OptionalProperty::Some(report.description.clone()),
            typ: OptionalProperty::Some(report.typ.clone()),
            query: OptionalProperty::Some(report.query.clone()),
        };

        let resp = cl
            .put(format!("{report_url}/1"))
            .body(serde_json::to_string(&req)?)
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::OK);
        let r: Report = serde_json::from_str(resp.text().await?.as_str())?;

        assert(&r, &report);
    }

    // get should return report
    {
        let resp = cl
            .get(format!("{report_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::OK);
        let r: Report = serde_json::from_str(resp.text().await?.as_str())?;
        assert(&r, &report);
    }
    // list reports should return list with one report
    {
        let resp = cl
            .get(&report_url)
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::OK);
        let resp: ListResponse<Report> = serde_json::from_str(resp.text().await?.as_str())?;
        assert_eq!(resp.data.len(), 1);
        assert(&resp.data[0], &report);
    }

    // delete request should delete report
    {
        let resp = cl
            .delete(format!("{report_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::OK);

        let resp = cl
            .delete(format!("{report_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await?;
        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }
    Ok(())
}