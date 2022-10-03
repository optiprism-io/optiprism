use axum::http::HeaderValue;
use axum::{AddExtensionLayer, http, Router, Server};
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
use platform::{AccountsProvider, AuthProvider, CustomEventsProvider, EventsProvider};
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use convert_case::Casing;
use tokio::time::{sleep};
use uuid::Uuid;
use common::rbac::{OrganizationRole, ProjectRole, Role};
use metadata::accounts::UpdateAccountRequest;
use platform::auth::token::{AccessClaims, make_access_token, make_password_hash};
use platform::auth::{SignUpRequest};
use platform::auth::types::TokensResponse;
use platform::http::auth::{LogInRequest, RefreshTokensRequest};

#[tokio::test]
async fn test_auth() -> anyhow::Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let md_accs = Arc::new(metadata::accounts::Provider::new(store.clone()));
    let md_accs_clone1 = md_accs.clone();
    let md_accs_clone2 = md_accs.clone();
    let md_events = Arc::new(metadata::events::Provider::new(store.clone()));
    let md_events_clone = md_events.clone();

    let access_token_secret = "access_secret".to_string();
    let refresh_token_secret = "refresh_secret".to_string();
    tokio::spawn(async {
        let events_prov = Arc::new(EventsProvider::new(md_events_clone));
        let auth_prov = Arc::new(AuthProvider::new(
            md_accs_clone1,
            Duration::days(1),
            access_token_secret,
            Duration::weeks(1),
            refresh_token_secret,
        ));


        let mut router = events::attach_routes(Router::new(), events_prov);
        router = auth::attach_routes(router, auth_prov);
        router = router.layer(AddExtensionLayer::new(md_accs_clone2));

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(router.into_make_service())
            .await
            .unwrap();
    });

    sleep(tokio::time::Duration::from_millis(100)).await;

    let pwd = "password".to_string();

    let admin = md_accs.create(metadata::accounts::CreateAccountRequest {
        created_by: Some(1),
        password_hash: make_password_hash(pwd.as_str())?.to_string(),
        email: "admin@mail.com".to_string(),
        first_name: None,
        last_name: None,
        role: None,
        organizations: Some(vec![(1, OrganizationRole::Admin)]),
        projects: None,
        teams: None,
    }).await?;

    let cl = Client::new();

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_str("application/json")?,
    );

    let admin_tokens = {
        let req = LogInRequest {
            email: admin.email.clone(),
            password: pwd.clone(),
        };

        let body = serde_json::to_string(&req)?;

        let resp = cl
            .post("http://127.0.0.1:8080/v1/auth/login")
            .body(body)
            .headers(headers.clone())
            .send()
            .await?;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: TokensResponse = serde_json::from_str(resp.text().await?.as_str())?;

        resp
    };


    let user_tokens = {
        let req = SignUpRequest {
            email: "user@gmail.com".to_string(),
            password: pwd.clone(),
            password_repeat: pwd.clone(),
            first_name: Some("first".to_string()),
            last_name: Some("last".to_string()),
        };

        let body = serde_json::to_string(&req)?;

        let mut admin_headers = headers.clone();
        admin_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", admin_tokens.access_token).as_str())?,
        );

        let resp = cl
            .post("http://127.0.0.1:8080/v1/auth/signup")
            .body(body)
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let resp: TokensResponse = serde_json::from_str(resp.text().await?.as_str())?;

        md_accs.update(2, UpdateAccountRequest {
            updated_by: 2,
            password: None,
            email: None,
            first_name: None,
            last_name: None,
            role: None,
            organizations: Some(Some(vec![(1, OrganizationRole::Member)])),
            projects: Some(Some(vec![(1, ProjectRole::Reader)])),
            teams: None,
        }).await?;
        resp
    };

    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events")
            .headers(headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // list without events should be empty
    {
        let mut jwt_headers = headers.clone();
        jwt_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", user_tokens.access_token).as_str())?,
        );

        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events")
            .headers(jwt_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().await?,
            r#"{"data":[],"meta":{"next":null}}"#
        );
    }

    let new_jwt_headers = {
        let req = RefreshTokensRequest {
            refresh_token: user_tokens.refresh_token,
        };

        let body = serde_json::to_string(&req)?;
        let resp = cl
            .post("http://127.0.0.1:8080/v1/auth/refresh-token")
            .body(body)
            .headers(headers.clone())
            .send()
            .await?;
        assert_eq!(resp.status(), StatusCode::OK);
        let new_user_tokens: TokensResponse = serde_json::from_str(resp.text().await?.as_str())?;

        // todo: check for tokens revocation here

        let mut new_jwt_headers = headers.clone();
        new_jwt_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", new_user_tokens.access_token).as_str())?,
        );

        new_jwt_headers
    };

    // list without events should be empty
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events")
            .headers(new_jwt_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().await?,
            r#"{"data":[],"meta":{"next":null}}"#
        );
    }

    Ok(())
}