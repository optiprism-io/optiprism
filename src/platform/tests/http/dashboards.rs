use common::types::OptionalProperty;
use platform::dashboards::CreateDashboardRequest;
use platform::dashboards::Dashboard;
use platform::dashboards::Panel;
use platform::dashboards::Type;
use platform::dashboards::UpdateDashboardRequest;
use platform::ListResponse;
use reqwest::Client;
use reqwest::StatusCode;

use crate::assert_response_json_eq;
use crate::assert_response_status_eq;
use crate::http::tests::create_admin_acc_and_login;
use crate::http::tests::run_http_service;
use crate::http::tests::EMPTY_LIST;

fn assert(l: &Dashboard, r: &Dashboard) {
    assert_eq!(l.id, 1);
    assert_eq!(l.project_id, r.project_id);
    assert_eq!(l.tags, r.tags);
    assert_eq!(l.name, r.name);
    assert_eq!(l.description, r.description);
    assert_eq!(l.panels, r.panels);
}

#[tokio::test]
async fn test_dashboards() {
    let (base_url, md, pp) = run_http_service(false).await.unwrap();
    let dash_url = format!("{base_url}/organizations/1/projects/1/dashboards");
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
        .await
        .unwrap();

    let mut dash = Dashboard {
        id: 0,
        created_at: Default::default(),
        updated_at: None,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        tags: Some(vec!["sdf".to_string()]),
        name: "qwe".to_string(),
        description: Some("desc".to_string()),
        panels: vec![Panel {
            typ: Type::Report,
            report_id: 1,
            x: 1,
            y: 2,
            w: 3,
            h: 4,
        }],
    };

    // list without dashboards should be empty
    {
        let resp = cl
            .get(&dash_url)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST.to_string());
    }

    // get of un-existing dashboard 1 should return 404 not found error
    {
        let resp = cl
            .get(format!("{dash_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // delete of un-existing dashboard 1 should return 404 not found error
    {
        let resp = cl
            .delete(format!("{dash_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // create request should create dashboard
    {
        let req = CreateDashboardRequest {
            tags: dash.tags.clone(),
            name: dash.name.clone(),
            description: dash.description.clone(),
            panels: dash.panels.clone(),
        };

        let resp = cl
            .post(&dash_url)
            .body(serde_json::to_string(&req).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::CREATED);
        let resp: Dashboard = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&resp, &dash)
    }

    // update request should update dashboard
    {
        dash.tags = Some(vec!["ert".to_string()]);
        dash.description = Some("xcv".to_string());
        dash.panels = vec![Panel {
            typ: Type::Report,
            report_id: 2,
            x: 3,
            y: 4,
            w: 5,
            h: 6,
        }];

        let req = UpdateDashboardRequest {
            tags: OptionalProperty::Some(dash.tags.clone()),
            name: OptionalProperty::Some(dash.name.clone()),
            description: OptionalProperty::Some(dash.description.clone()),
            panels: OptionalProperty::Some(dash.panels.clone()),
        };

        let resp = cl
            .put(format!("{dash_url}/1"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let d: Dashboard = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();

        assert(&d, &dash);
    }

    // get should return dashboard
    {
        let resp = cl
            .get(format!("{dash_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let d: Dashboard = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&d, &dash);
    }
    // list dashboards should return list with one dashboard
    {
        let resp = cl
            .get(&dash_url)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let resp: ListResponse<Dashboard> =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert(&resp.data[0], &dash);
    }

    // delete request should delete dashboard
    {
        let resp = cl
            .delete(format!("{dash_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);

        let resp = cl
            .delete(format!("{dash_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }
}
