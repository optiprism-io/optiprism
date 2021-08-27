mod account;
mod auth;
mod context;
mod error;
mod http;
mod organization;
mod rbac;
mod dbutils;

use actix_http::{header, HttpMessage};
use actix_service::Service;
use actix_web::{dev::Server, web::Data, App, HttpServer};
use context::{Context, ContextExtractor};
use rocksdb::DB;
use std::{env::var, io::Result, sync::Arc};

pub fn init(db: Arc<DB>) -> Result<Server> {
    let organization_provider = Data::new(organization::Provider::new(db.clone()));
    let account_provider = Data::new(account::Provider::new(db.clone()));
    let auth_provider = Data::new(auth::Provider::new(
        organization_provider.clone().into_inner(),
        account_provider.clone().into_inner(),
    ));
    Ok(HttpServer::new(move || {
        App::new()
            .wrap_fn(|request, service| {
                request
                    .extensions_mut()
                    .insert(ContextExtractor::new(Context::from_token(
                        request.headers().get(header::AUTHORIZATION),
                    )));
                service.call(request)
            })
            .app_data(organization_provider.clone())
            .app_data(account_provider.clone())
            .app_data(auth_provider.clone())
            .configure(http::configure)
    })
    .bind(var("FNP_BIND_ADDRESS").unwrap())?
    .run())
}
