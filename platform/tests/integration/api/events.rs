use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{thread, time};
use axum::{AddExtensionLayer, Router, Server};
use reqwest::{Client, StatusCode};
use uuid::Uuid;
use metadata::{Metadata, Store};
use platform::{events::Provider as EventsProvider, http};
use platform::error::Result;
use tokio::time::{sleep, Duration};
use platform::http::events;

#[tokio::test]
async fn test_events() -> Result<()> {
    let handle = tokio::spawn(async {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        let store = Arc::new(Store::new(path));
        let metadata = Arc::new(Metadata::try_new(store).unwrap());
        let events_provider = Arc::new(EventsProvider::new(metadata.clone()));

        let app =
            events::configure(Router::new())
                .layer(AddExtensionLayer::new(events_provider));

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(app.into_make_service())
            .await.unwrap();
    });


    sleep(Duration::from_millis(100)).await;

    let cl = Client::new();
    /*{
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events").send().await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), r#"{"data":[],"meta":{"next":null}}"#);
    }*/

    {
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events/1").send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    /*
        {
            let resp = cl.delete("http://127.0.0.1:8080/v1/projects/1/events/1").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }

        {
            let resp = cl.put("http://127.0.0.1:8080/v1/projects/1/events/1").body("").send().await.unwrap();
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }*/

    {
        let body = r#"
        {
        }
        "#;
        let resp = cl.post("http://127.0.0.1:8080/v1/projects/1/events").body(body).send().await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), r#"{"data":[],"meta":{"next":null}}"#);
    }

    Ok(())
}