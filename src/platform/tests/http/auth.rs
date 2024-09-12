use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::types::OptionalProperty;
use metadata::accounts::UpdateAccountRequest;
use platform::accounts::Account;
use platform::auth::provider::SignUpRequest;
use platform::auth::provider::TokensResponse;
use platform::auth::provider::UpdateEmailRequest;
use platform::auth::provider::UpdateNameRequest;
use platform::auth::provider::UpdatePasswordRequest;
use platform::http::auth::RefreshTokenRequest;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::AUTHORIZATION;
use reqwest::header::CONTENT_TYPE;
use reqwest::Client;
use reqwest::StatusCode;

use crate::assert_response_json_eq;
use crate::assert_response_status_eq;
use crate::http::tests::{create_admin_acc_and_login, init_settings};
use crate::http::tests::run_http_service;
use crate::http::tests::EMPTY_LIST;

#[tokio::test]
async fn test_auth() {
    let (base_addr, md, pp) = run_http_service(false).await.unwrap();
    let auth_addr = format!("{base_addr}/auth");
    let cl = Client::new();
    init_settings(&md);
    // login to admin acc
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
        .await
        .unwrap();

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );

    let user_tokens: TokensResponse = {
        let pwd = "1password123#@E".to_string();
        let req = SignUpRequest {
            email: "user@gmail.com".to_string(),
            password: pwd.clone(),
            password_repeat: pwd.clone(),
            name: Some("name".to_string()),
        };

        // signup
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
                password_hash: OptionalProperty::None,
                email: OptionalProperty::None,
                name: OptionalProperty::None,
                force_update_password: Default::default(),
                force_update_email: Default::default(),
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
            .get(format!("{base_addr}/projects/1/schema/events"))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::UNAUTHORIZED);
    }

    // list without accounts should be empty
    {
        let mut jwt_headers = headers.clone();
        jwt_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", user_tokens.access_token).as_str()).unwrap(),
        );

        let resp = cl
            .get(format!("{base_addr}/projects/1/schema/events"))
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
            AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", new_user_tokens.access_token).as_str())
                .unwrap(),
        );

        new_jwt_headers
    };

    // get should return profile
    {
        let resp = cl
            .get(format!("{auth_addr}/profile"))
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let acc: Account = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(acc.name, Some("name".to_string()));
    }

    // list without accounts should be empty
    {
        let resp = cl
            .get(format!("{base_addr}/projects/1/schema/events"))
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST.to_string());
    }

    // update name
    {
        let req = UpdateNameRequest {
            name: "new name".to_string(),
        };

        let resp = cl
            .put(format!("{auth_addr}/profile/name"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
    }

    // update email with wrong password
    {
        let req = UpdateEmailRequest {
            email: "new@test.com".to_string(),
            password: "wrong".to_string(),
        };

        let resp = cl
            .put(format!("{auth_addr}/profile/email"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::UNAUTHORIZED);
    }

    // update email
    {
        let req = UpdateEmailRequest {
            email: "new@test.com".to_string(),
            password: "password".to_string(),
        };

        let resp = cl
            .put(format!("{auth_addr}/profile/email"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
    }

    // update password with wrong password
    {
        let req = UpdatePasswordRequest {
            password: "wrong".to_string(),
            new_password: "new".to_string(),
        };

        let resp = cl
            .put(format!("{auth_addr}/profile/password"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::UNAUTHORIZED);
    }

    // update password
    {
        let req = UpdatePasswordRequest {
            password: "password".to_string(),
            new_password: "new".to_string(),
        };

        let resp = cl
            .put(format!("{auth_addr}/profile/password"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
    }

    // get should return updated profile
    {
        let resp = cl
            .get(format!("{auth_addr}/profile"))
            .headers(new_jwt_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let acc: Account = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(acc.name, Some("new name".to_string()));
        assert_eq!(acc.email, "new@test.com".to_string());
    }
}
