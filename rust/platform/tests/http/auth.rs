use axum::http;
use axum::http::HeaderValue;

use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};

use common::rbac::{OrganizationRole, ProjectRole};
use metadata::accounts::UpdateAccountRequest;

use common::types::OptionalProperty;

use platform::auth::types::TokensResponse;
use platform::auth::SignUpRequest;
use platform::http::auth::RefreshTokenRequest;

use crate::http::tests::{create_admin_acc_and_login, run_http_service};

#[tokio::test]
async fn test_auth() -> anyhow::Result<()> {
    let (base_addr, md, pp) = run_http_service(false).await?;
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_str("application/json")?,
    );

    let user_tokens = {
        let pwd = "password".to_string();
        let req = SignUpRequest {
            email: "user@gmail.com".to_string(),
            password: pwd.clone(),
            password_repeat: pwd.clone(),
            first_name: Some("first".to_string()),
            last_name: Some("last".to_string()),
        };

        let body = serde_json::to_string(&req)?;

        let resp = cl
            .post(format!("{base_addr}/api/v1/auth/signup"))
            .body(body)
            .headers(admin_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let resp: TokensResponse = serde_json::from_str(resp.text().await?.as_str())?;

        md.accounts
            .update(
                2,
                UpdateAccountRequest {
                    updated_by: 2,
                    password: OptionalProperty::None,
                    email: OptionalProperty::None,
                    first_name: OptionalProperty::None,
                    last_name: OptionalProperty::None,
                    role: OptionalProperty::None,
                    organizations: OptionalProperty::Some(Some(vec![(
                        1,
                        OrganizationRole::Member,
                    )])),
                    projects: OptionalProperty::Some(Some(vec![(1, ProjectRole::Reader)])),
                    teams: OptionalProperty::None,
                },
            )
            .await?;
        resp
    };

    {
        let resp = cl
            .get(format!(
                "{base_addr}/api/v1/organizations/1/projects/1/schema/events"
            ))
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
            .get(format!(
                "{base_addr}/api/v1/organizations/1/projects/1/schema/events"
            ))
            .headers(jwt_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await?, r#"{"data":[],"meta":{"next":null}}"#);
    }

    let new_jwt_headers = {
        let req = RefreshTokenRequest {
            refresh_token: Some(user_tokens.refresh_token),
        };

        let body = serde_json::to_string(&req)?;
        let resp = cl
            .post(format!("{base_addr}/api/v1/auth/refresh-token"))
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
            .get(format!(
                "{base_addr}/api/v1/organizations/1/projects/1/schema/events"
            ))
            .headers(new_jwt_headers.clone())
            .send()
            .await?;

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await?, r#"{"data":[],"meta":{"next":null}}"#);
    }

    Ok(())
}
