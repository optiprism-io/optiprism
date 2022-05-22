use std::path::PathBuf;
use chrono::{DateTime, Utc};
use rand::thread_rng;
use events_gen::generator::Generator;
use events_gen::store::products::{Preferences, Products};
use events_gen::store::scenario::{RecordBatchBuilder, run};

fn main() {
    let mut rng = thread_rng();
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/store/data/products.csv");
    let mut products = Products::try_new_from_csv(path, &mut rng).unwrap();
    let preferences = Preferences {
        categories: None,
        subcategories: None,
        brand: None,
        author: None,
        size: None,
        color: None,
        max_price: None,
        min_price: None,
        min_rating: None,
        has_coupon: true,
    };

    let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339("2022-09-08T14:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let mut gen = Generator::new(
        rng.clone(),
        from,
        to.clone(),
        5,
    );
    let mut builder = RecordBatchBuilder::new();
    run(&mut gen, &mut products, to, &mut builder, rng.clone()).unwrap();
}