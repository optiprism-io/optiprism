mod account;
mod context;
mod error;
mod organization;
mod auth;

use actix_web::{get, web::Data, App, HttpServer};
use rocksdb::DB;
use std::env::var;
use std::sync::Arc;

#[get("/")]
async fn index() -> &'static str {
    "Hello, World!"
}

async fn init(db: Arc<DB>) -> std::io::Result<()> {
    let organization_provider = Data::new(organization::Provider::new(db.clone()));
    let account_provider = Data::new(account::Provider::new(db.clone()));
    // let user_provider = web::Data::new(providers::user::Service::new(pool.clone()).await.unwrap());
    // let auth_provider = web::Data::new(providers::auth::Service::new(
    //     user_provider.clone().into_inner(),
    // ));
    HttpServer::new(move || {
        App::new()
        .wrap_fn(context::middleware)
            .configure(context::configure)
            .app_data(organization_provider.clone())
            .app_data(account_provider.clone())
            .configure(http::configure)
    })
    .bind(var("FNP_BIND_ADDRESS").unwrap())?
    .run()
    .await
}
