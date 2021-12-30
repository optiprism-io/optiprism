mod app;
mod exprtree;

use rocksdb::{ColumnFamilyDescriptor, Options, DB};
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

    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = Arc::new(
        DB::open_cf_descriptors(&options, var("FNP_DB_PATH").unwrap(), app::get_cfs()).unwrap(),
    );

    app::init(db)?.await
}
