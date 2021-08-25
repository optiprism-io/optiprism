use actix_web::{get, web, App, HttpServer};
use std::env::var;

#[get("/")]
async fn index() -> &'static str {
    "Hello, World!"
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // let user_service = web::Data::new(services::user::Service::new(pool.clone()).await.unwrap());
    // let auth_service = web::Data::new(services::auth::Service::new(
    //     user_service.clone().into_inner(),
    // ));

    HttpServer::new(move || {
        App::new()
            .service(index)
            // .app_data(user_service.clone())
            // .configure(services::user::endpoints)
            // .app_data(auth_service.clone())
            // .configure(services::auth::endpoints)
    })
    .bind(var("ET_BIND_ADDRESS").unwrap())?
    .run()
    .await
}
