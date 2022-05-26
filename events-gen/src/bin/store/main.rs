use std::path::PathBuf;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use rand::thread_rng;
use events_gen::generator::Generator;
use events_gen::store::products::{Preferences, Products};
use events_gen::store::scenario::{RecordBatchBuilder, run};
use metadata::database::{Column, TableType};
use metadata::Metadata;
use metadata::properties::{CreatePropertyRequest, Property};
use metadata::properties::provider::Namespace;

async fn create_property(
    md: &Arc<Metadata>,
    ns: Namespace,
    org_id: u64,
    proj_id: u64,
    req: CreatePropertyRequest,
) -> Result<Property> {
    let prop = match ns {
        Namespace::Event => md.event_properties.create(org_id, req).await?,
        Namespace::User => md.user_properties.create(org_id, req).await?,
    };

    md.database
        .add_column(
            TableType::Events(org_id, proj_id),
            Column::new(prop.column_name(ns), prop.typ.clone(), prop.nullable, prop.dictionary_type.clone()),
        )
        .await?;

    Ok(prop)
}

fn main() {
    let mut rng = thread_rng();
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("src/store/data/products.csv");
    let mut products = Products::try_new_from_csv(path, &mut rng).unwrap();

    let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339("2022-09-08T14:42:00.000000+00:00").unwrap().with_timezone(&Utc);
    let hour_weights = [0.4, 0.37, 0.39, 0.43, 0.45, 0.47, 0.52, 0.6, 0.8, 0.9, 0.85, 0.8, 0.75, 0.85, 1., 0.85, 0.7, 0.63, 0.62, 0.61, 0.59, 0.57, 0.48, 0.4];
    let mut gen = Generator::new(
        rng.clone(),
        from,
        to.clone(),
        30,
        &hour_weights,
    );
    run(rng, &mut gen, &mut products, to, 10_000).unwrap();
}