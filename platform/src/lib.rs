pub mod common;
pub mod error;
// pub mod http;
pub mod event;

pub const SEQUENCE_NAME: &str = "sequence";

mod test {
    use crate::{
        common::sequence::{Sequence, SequenceBuilder},
        error::Result,
        event, SEQUENCE_NAME,
    };
    use rocksdb::{ColumnFamilyDescriptor, Options, DB};
    use std::{
        env::{set_var, var},
        sync::Arc,
    };

    #[test]
    fn init() {
        set_var("FNP_DB_PATH", "./.db");

        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let common_cfs = vec![ColumnFamilyDescriptor::new(
            SEQUENCE_NAME,
            Options::default(),
        )];
        let cfs = common_cfs
            .into_iter()
            .chain(event::Provider::cfs().into_iter());

        let db = DB::open_cf_descriptors(&options, var("FNP_DB_PATH").unwrap(), cfs).unwrap();
        let sequence_builder = SequenceBuilder::new(&db, db.cf_handle(SEQUENCE_NAME).unwrap());

        let event_provider = event::Provider::new(&db, &sequence_builder);
        // event_provider.create(event)
    }
}
