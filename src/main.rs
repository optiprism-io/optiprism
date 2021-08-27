mod app;
mod exprtree;

use rocksdb::DB;
use std::{env::var, sync::Arc};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let db = Arc::new(DB::open_default(var("ET_DB_PATH").unwrap()).unwrap());

    app::init(db)?.await
}
