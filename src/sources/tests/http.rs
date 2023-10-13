//! HTTP ingester source tests.

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
use ingester::pipeline::Ingester;
use lazy_static::lazy_static;
use metadata::atomic_counters::ProviderImpl as AtomicCountersProviderImpl;
use metadata::error::EventError;
use metadata::error::MetadataError;
use metadata::error::PropertyError;
use metadata::events::Provider as _;
use metadata::events::ProviderImpl as EventsProviderImpl;
use metadata::properties::Provider as _;
use metadata::properties::ProviderImpl as PropertiesProviderImpl;
use metadata::store::Store;
use serde_json::json;
use sources::http::attach_routes;
use sources::http::TrackResponse;
use util::tmp_store;

const EVENT_NAME: &str = "Buy Product";
const ORGANIZATION_ID: u64 = 1;
const PROJECT_ID: u64 = 2;

lazy_static! {
    static ref EVENT_PROPERTIES: serde_json::Value = json!({
        "Product Name": "Samsung TV",
        "Product Size": 60,
        "HDR": true,
        "Price": 899
    });

    static ref USER_PROPERTIES: serde_json::Value = json!({
        "Email": "sdf@asdf.com"
    });

    static ref TRACKING_REQUEST_BODY: String = json!({
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
        "event": EVENT_NAME,
        "properties": EVENT_PROPERTIES.clone(),
        "userProperties": USER_PROPERTIES.clone()
    }).to_string();
}

fn make_service(store: Arc<Store>) -> Router {
    let atomic_counters_provider = Arc::new(AtomicCountersProviderImpl::new(store.clone()));
    let events_metadata_provider = Arc::new(EventsProviderImpl::new(store.clone()));
    let event_properties_metadata_provider =
        Arc::new(PropertiesProviderImpl::new_event(store.clone()));
    let user_properties_metadata_provider =
        Arc::new(PropertiesProviderImpl::new_user(store.clone()));

    let ingester = Ingester {
        events_metadata_provider,
        event_properties_metadata_provider,
        user_properties_metadata_provider,
        atomic_counters_provider,
    };

    attach_routes(Router::new()).layer(Extension(Arc::new(ingester)))
}

async fn do_good_request(service: &mut Router) -> Response {
    let request = Request::post("/organizations/1/projects/2/track/events")
        .header(header::CONTENT_TYPE, "application/json")
        .body(TRACKING_REQUEST_BODY.clone().into())
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
    let request = Request::post(format!(
        "/organizations/{ORGANIZATION_ID}/projects/{PROJECT_ID}/track/events"
    ))
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

#[tokio::test]
async fn missing_event_metadata_created() {
    let store = tmp_store();

    // Ensure there is no event metadata:
    let event_metadata_provider = EventsProviderImpl::new(store.clone());
    assert!(matches!(
        event_metadata_provider
            .get_by_name(ORGANIZATION_ID, PROJECT_ID, EVENT_NAME)
            .await,
        Err(MetadataError::Event(EventError::EventNotFound(_)))
    ));

    let mut service = make_service(store);

    let response1 = do_good_request(&mut service).await;
    assert_eq!(response1.status(), StatusCode::CREATED);

    assert!(matches!(
        event_metadata_provider
            .get_by_name(ORGANIZATION_ID, PROJECT_ID, EVENT_NAME)
            .await,
        Ok(_)
    ));
}

#[tokio::test]
async fn missing_event_properties_metadata_created() {
    let store = tmp_store();

    // Ensure there is no event metadata:
    let event_properties_metadata_provider = PropertiesProviderImpl::new_event(store.clone());

    for name in EVENT_PROPERTIES.as_object().unwrap().keys() {
        assert!(matches!(
            event_properties_metadata_provider
                .get_by_name(ORGANIZATION_ID, PROJECT_ID, name)
                .await,
            Err(MetadataError::Property(PropertyError::PropertyNotFound(_)))
        ));
    }

    let mut service = make_service(store);

    let response1 = do_good_request(&mut service).await;
    assert_eq!(response1.status(), StatusCode::CREATED);

    for name in EVENT_PROPERTIES.as_object().unwrap().keys() {
        assert!(matches!(
            event_properties_metadata_provider
                .get_by_name(ORGANIZATION_ID, PROJECT_ID, name)
                .await,
            Ok(_)
        ));
    }
}

#[tokio::test]
async fn missing_user_properties_metadata_created() {
    let store = tmp_store();

    // Ensure there is no user metadata:
    let user_properties_metadata_provider = PropertiesProviderImpl::new_user(store.clone());

    for name in USER_PROPERTIES.as_object().unwrap().keys() {
        assert!(matches!(
            user_properties_metadata_provider
                .get_by_name(ORGANIZATION_ID, PROJECT_ID, name)
                .await,
            Err(MetadataError::Property(PropertyError::PropertyNotFound(_)))
        ));
    }

    let mut service = make_service(store);

    let response1 = do_good_request(&mut service).await;
    assert_eq!(response1.status(), StatusCode::CREATED);

    for name in USER_PROPERTIES.as_object().unwrap().keys() {
        assert!(matches!(
            user_properties_metadata_provider
                .get_by_name(ORGANIZATION_ID, PROJECT_ID, name)
                .await,
            Ok(_)
        ));
    }
}
