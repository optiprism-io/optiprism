mod error;

use axum::{Router, Server};
use metadata::{Metadata, Store};
use platform::{accounts::Provider as AccountProvider, auth::Provider as AuthProvider, events::Provider as EventsProvider, properties::Provider as PropertiesProvider};
use std::{env::set_var, net::SocketAddr, sync::Arc};
use std::path::PathBuf;
use tower_http::add_extension::AddExtensionLayer;
use platform::platform::Platform;
use error::Result;
use query::QueryProvider;
use crate::error::Error;

#[tokio::main]
async fn main() -> Result<()> {
    // test env
    {
        set_var("FNP_COMMON_SALT", "FNP_COMMON_SALT");
        set_var("FNP_EMAIL_TOKEN_KEY", "FNP_EMAIL_TOKEN_KEY");
        set_var("FNP_ACCESS_TOKEN_KEY", "FNP_ACCESS_TOKEN_KEY");
        set_var("FNP_REFRESH_TOKEN_KEY", "FNP_REFRESH_TOKEN_KEY");
    }

    let store = Arc::new(Store::new("db"));
    let md = Arc::new(Metadata::try_new(store)?);

    let prov = {
        let mut rng = thread_rng();
        let root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let mut products_path = root_path.clone();
        products_path.push("src/store/data/products.csv");

        let mut geo_path = root_path.clone();
        geo_path.push("src/data/geo.csv");

        let mut device_path = root_path.clone();
        device_path.push("src/data/device.csv");

        cfg = store::Config{
            org_id: 0,
            project_id: 0,
            md: Arc::new(Metadata {}),
            dicts: Arc::new(()),
            from: (),
            to: (),
            products_path: (),
            geo_path: (),
            device_path: (),
            new_daily_users: 0,
            batch_size: 0,
            partitions: 0
        };

        store::gen(cfg);
    };

    let query_provider = Arc::new(QueryProvider::try_new(md.clone())?);
    let platform = platform::Platform::new(md.clone(), query_provider);

    let mut router = Router::new();
    router = platform::http::attach_routes(router, platform);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    Server::bind(&addr)
        .serve(router.into_make_service())
        .await.map_err(|e| Error::ExternalError(e.to_string()));
    Ok(())
}
