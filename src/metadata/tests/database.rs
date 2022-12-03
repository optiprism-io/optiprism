use std::env::temp_dir;
use std::sync::Arc;

use arrow::datatypes::DataType;
use metadata::database::Column;
use metadata::database::Provider;
use metadata::database::ProviderImpl;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::error::Result;
use metadata::store::Store;
use uuid::Uuid;

#[tokio::test]
async fn test_database() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let db: Box<dyn Provider> = Box::new(ProviderImpl::new(store.clone()));

    let table = Table {
        typ: TableRef::System("t1".to_string()),
        columns: vec![],
    };

    // create table
    assert!(db.create_table(table.clone()).await.is_ok());
    // table already exists
    assert!(db.create_table(table.clone()).await.is_err());

    // un-existent table
    assert!(
        db.get_table(TableRef::System("nx".to_string()))
            .await
            .is_err()
    );
    // get table by name
    assert_eq!(db.get_table(table.typ.clone()).await?, table);

    let col = Column {
        name: "c1".to_string(),
        data_type: DataType::Null,
        nullable: false,
        dictionary: None,
    };

    // add column, non-existent table
    assert!(
        db.add_column(TableRef::System("nx".to_string()), col.clone())
            .await
            .is_err()
    );
    // add column
    assert!(
        db.add_column(TableRef::System("t1".to_string()), col.clone())
            .await
            .is_ok()
    );
    // column already exist
    assert!(
        db.add_column(TableRef::System("t1".to_string()), col.clone())
            .await
            .is_err()
    );
    Ok(())
}
