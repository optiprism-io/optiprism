use std::net::SocketAddr;
use std::sync::Arc;
use axum::{AddExtensionLayer, Router, Server};
use metadata::{Metadata, Store};
use platform::{events::Provider as EventsProvider};
use platform::error::Result;

#[tokio::test]
async fn test_events() -> Result<()> {
    let store = Arc::new(Store::new(".db"));
    let metadata = Arc::new(Metadata::try_new(store)?);
    let events_provider = Arc::new(EventsProvider::new(metadata.clone()));

    let app = platform::http::configure(Router::new())
        .layer(AddExtensionLayer::new(events_provider));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}