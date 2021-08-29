mod app;
mod exprtree;
mod user_storage;

use rocksdb::DB;
use std::{env::var, sync::Arc};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    {
        // test
        std::env::set_var("FNP_DB_PATH", "./.db");
        std::env::set_var("FNP_BIND_ADDRESS", "127.0.0.1:8080");
        std::env::set_var("FNP_COMMON_SALT", "salt");
        std::env::set_var("FNP_EMAIL_TOKEN_KEY", "key");
        std::env::set_var("FNP_ACCESS_TOKEN_KEY", "key");
        std::env::set_var("FNP_REFRESH_TOKEN_KEY", "key");
    }

    let db = Arc::new(DB::open_default(var("FNP_DB_PATH").unwrap()).unwrap());

    app::init(db)?.await
}
