use axum::http::HeaderValue;
use axum::{http, Router, Server};
use chrono::{Duration, Utc};
use metadata::metadata::ListResponse;
use metadata::store::Store;
use platform::error::Result;

use metadata::custom_events::Provider;
use platform::custom_events::types::{
    CreateCustomEventRequest, CustomEvent, Event, Status, UpdateCustomEventRequest,
};
use platform::http::{auth, custom_events, events};
use platform::queries::types::EventRef;
use platform::{AccountsProvider, CustomEventsProvider, EventsProvider};
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep};
use uuid::Uuid;
use platform::auth::auth::{AccessClaims, make_access_token, make_password_hash};
use platform::auth::LogInRequest;
use platform::auth::types::TokenResponse;

#[tokio::test]
async fn test_custom_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let md_accs = Arc::new(metadata::accounts::Provider::new(store.clone()));

    tokio::spawn(async {
        let md_events = Arc::new(metadata::events::Provider::new(store.clone()));
        let events_prov = Arc::new(EventsProvider::new(md_events));
        let auth_prov = Arc::new(metadata::auth::Provider::new(store.clone()));

        let mut router = events::attach_routes(Router::new(), events_prov);
        router = auth::attach_routes(router, auth_prov);
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    sleep(tokio::time::Duration::from_millis(100)).await;


    let pwd = "password".to_string();
    let acc1 = md_accs.create(metadata::accounts::CreateAccountRequest {
        created_by: 0,
        password_hash: make_password_hash(pwd.as_str())?.to_string(),
        email: "1@mail.com".to_string(),
        first_name: None,
        last_name: None,
        role: None,
        organizations: None,
        projects: None,
        teams: None,
    }).await?;

    let claims = AccessClaims { exp: 0, account_id: acc1.id };
    let token_key = "token";

    let cl = Client::new();

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );

    let access_token = {
        let req = LogInRequest {
            email: acc1.email,
            password: pwd,
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .post("http://127.0.0.1:8080/v1/auth/login")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let resp: TokenResponse = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();

        resp.access_token
    };

    let mut jwt_headers = headers.clone();
    jwt_headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(format!("bearer {}", token).as_str()).unwrap(),
    );

    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events")
            .headers(jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // list without events should be empty
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events")
            .headers(jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().await.unwrap(),
            r#"{"data":[],"meta":{"next":null}}"#
        );
    }

    Ok(())
}