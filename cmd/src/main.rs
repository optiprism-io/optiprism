use axum::{Router, Server};
use metadata::{Metadata, Store};
use platform::auth::Provider as AuthProvider;
use std::{env::set_var, net::SocketAddr, sync::Arc};
use tower_http::add_extension::AddExtensionLayer;

#[tokio::main]
async fn main() {
    // test env
    {
        set_var("FNP_COMMON_SALT", "FNP_COMMON_SALT");
        set_var("FNP_EMAIL_TOKEN_KEY", "FNP_EMAIL_TOKEN_KEY");
        set_var("FNP_ACCESS_TOKEN_KEY", "FNP_ACCESS_TOKEN_KEY");
        set_var("FNP_REFRESH_TOKEN_KEY", "FNP_REFRESH_TOKEN_KEY");
    }

    let store = Arc::new(Store::new(".db"));
    let metadata = Arc::new(Metadata::try_new(store).unwrap());
    let auth_provider = AuthProvider::new(metadata);

    let app = platform::http::configure(Router::new())
        .layer(AddExtensionLayer::new(Arc::new(auth_provider)));

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
