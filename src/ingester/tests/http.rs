mod util;

use std::sync::Arc;

use axum::response::Response;
use axum::Extension;
use axum::Router;
use hyper::body;
use hyper::header;
use hyper::service::Service;
use hyper::Request;
use hyper::StatusCode;
use ingester::attach_routes;
use ingester::ingester::TrackResponse;
use metadata::atomic_counters::Provider as AtomicCountersProvider;
use metadata::atomic_counters::ProviderImpl as AtomicCountersProviderImpl;
use metadata::events::Provider as EventsProvider;
use metadata::events::ProviderImpl as EventsProviderImpl;
use metadata::properties::Provider as PropertiesProvider;
use metadata::properties::ProviderImpl as PropertiesProviderImpl;
use metadata::store::Store;
use util::tmp_store;

const TRACKING_REQUEST_BODY: &'static str = r#"{
  "userId": "qwe123",
  "sentAt": "2015-12-12T19:11:01.169Z",
  "context": {
    "library": {
      "name": "analytics.js",
      "version": "2.11.1"
    },
    "page": {
      "path": "/search/",
      "referrer": "https://google.com",
      "search": "tv",
      "title": "Search Products",
      "url": "https://myshop.com/search/"
    },
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",
    "ip": "101.10.8.21"
  },
  "event": "Buy Product",
  "properties": {
    "Product Name": "Samsung TV",
    "Product Size": 60,
    "HDR": true,
    "Price": 899
  },
  "userProperties": {
    "Email": "sdf@asdf.com"
  }
}"#;

fn make_service(store: Arc<Store>) -> Router {
    attach_routes(Router::new())
        .layer(Extension(
            Arc::new(AtomicCountersProviderImpl::new(store.clone()))
                as Arc<dyn AtomicCountersProvider>,
        ))
        .layer(Extension(
            Arc::new(EventsProviderImpl::new(store.clone())) as Arc<dyn EventsProvider>
        ))
        .layer(Extension(
            Arc::new(PropertiesProviderImpl::new_event(store)) as Arc<dyn PropertiesProvider>,
        ))
}

async fn do_good_request(service: &mut Router) -> Response {
    let request = Request::post("/organizations/1/projects/2/track/events")
        .header(header::CONTENT_TYPE, "application/json")
        .body(TRACKING_REQUEST_BODY.into())
        .expect("cannot create request");

    service
        .call(request)
        .await
        .expect("there must be a response")
}

async fn to_track_response(response: Response) -> TrackResponse {
    let response_body = body::to_bytes(response)
        .await
        .expect("cannot collect response body");
    serde_json::from_reader(&*response_body).expect("cannot deserialize from json")
}

#[tokio::test]
async fn good_request_accepted() {
    let store = tmp_store();
    let mut service = make_service(store);

    let response = do_good_request(&mut service).await;
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn bad_request_rejected() {
    let store = tmp_store();
    let mut service = make_service(store);
    let request = Request::post("/organizations/1/projects/2/track/events")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"yeet": []}"#.into())
        .expect("cannot create request");
    let response = service
        .call(request)
        .await
        .expect("there must be a response");

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn event_record_ids_increase() {
    let store = tmp_store();
    let mut service = make_service(store);

    let response1 = do_good_request(&mut service).await;
    assert_eq!(response1.status(), StatusCode::CREATED);
    assert_eq!(to_track_response(response1).await.id, 1);

    let response2 = do_good_request(&mut service).await;
    assert_eq!(response2.status(), StatusCode::CREATED);
    assert_eq!(to_track_response(response2).await.id, 2);

    let response3 = do_good_request(&mut service).await;
    assert_eq!(response3.status(), StatusCode::CREATED);
    assert_eq!(to_track_response(response3).await.id, 3);
}
