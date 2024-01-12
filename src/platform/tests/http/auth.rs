use axum::http;
use axum::http::HeaderValue;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::types::OptionalProperty;
use metadata::accounts::UpdateAccountRequest;
use platform::auth::SignUpRequest;
use platform::auth::TokensResponse;
use platform::http::auth::RefreshTokenRequest;
use reqwest::header::HeaderMap;
use reqwest::Client;
use reqwest::StatusCode;

use crate::assert_response_json_eq;
use crate::assert_response_status_eq;
use crate::http::tests::create_admin_acc_and_login;
use crate::http::tests::run_http_service;
use crate::http::tests::EMPTY_LIST;

#[tokio::test]
async fn test_auth() {
    let (base_addr, md, pp) = run_http_service(false).await.unwrap();
    let auth_addr = format!("{base_addr}/auth");
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
        .await
        .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );

    let user_tokens: TokensResponse = {
        let pwd = "password".to_string();
        let req = SignUpRequest {
            email: "user@gmail.com".to_string(),
            password: pwd.clone(),
            password_repeat: pwd.clone(),
            first_name: Some("first".to_string()),
            last_name: Some("last".to_string()),
        };

        let resp = cl
            .post(format!("{auth_addr}/signup"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::CREATED);

        md.accounts
            .update(2, UpdateAccountRequest {
                updated_by: 2,
                password: OptionalProperty::None,
                email: OptionalProperty::None,
                first_name: OptionalProperty::None,
                last_name: OptionalProperty::None,
                role: OptionalProperty::None,
                organizations: OptionalProperty::Some(Some(vec![(1, OrganizationRole::Member)])),
                projects: OptionalProperty::Some(Some(vec![(1, ProjectRole::Reader)])),
                teams: OptionalProperty::None,
            })
            .unwrap();

        resp.json().await.unwrap()
    };

    {
        let resp = cl
            .get(format!(
                "{base_addr}/organizations/1/projects/1/schema/events"
            ))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::UNAUTHORIZED);
    }

    // list without events should be empty
    {
        let mut jwt_headers = headers.clone();
        jwt_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", user_tokens.access_token).as_str()).unwrap(),
        );

        let resp = cl
            .get(format!(
                "{base_addr}/organizations/1/projects/1/schema/events"
            ))
            .headers(jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST);
    }

    let new_jwt_headers = {
        let req = RefreshTokenRequest {
            refresh_token: Some(user_tokens.refresh_token),
        };

        let resp = cl
            .post(format!("{auth_addr}/refresh-token"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        let new_user_tokens: TokensResponse =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();

        // todo: check for tokens revocation here

        let mut new_jwt_headers = headers.clone();
        new_jwt_headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", new_user_tokens.access_token).as_str())
                .unwrap(),
        );

        new_jwt_headers
    };

    // list without events should be empty
    {
        let resp = cl
            .get(format!(
                "{base_addr}/organizations/1/projects/1/schema/events"
            ))
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST.to_string());
    }
}
