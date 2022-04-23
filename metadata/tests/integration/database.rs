use arrow::datatypes::DataType;
use metadata::database::{Column, Provider, Table, TableType};
use metadata::error::Result;
use metadata::Store;
use std::env::temp_dir;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_database() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let db = Provider::new(store.clone());

    let table = Table {
        typ: TableType::System("t1".to_string()),
        columns: vec![],
    };

    // create table
    assert!(db.create_table(table.clone()).await.is_ok());
    // table already exists
    assert!(db.create_table(table.clone()).await.is_err());

    // un-existent table
    assert!(db
        .get_table(TableType::System("nx".to_string()))
        .await
        .is_err());
    // get table by name
    assert_eq!(db.get_table(table.typ.clone()).await?, table);

    let col = Column {
        name: "c1".to_string(),
        data_type: DataType::Null,
        nullable: false,
    };

    // add column, non-existent table
    assert!(db
        .add_column(TableType::System("nx".to_string()), col.clone())
        .await
        .is_err());
    // add column
    assert!(db
        .add_column(TableType::System("t1".to_string()), col.clone())
        .await
        .is_ok());
    // column already exist
    assert!(db
        .add_column(TableType::System("t1".to_string()), col.clone())
        .await
        .is_err());
    Ok(())
}
