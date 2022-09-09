use metadata::store::Store;
use metadata::{
    organizations::{CreateRequest, ListRequest},
    Metadata, Result,
};
use std::{env::temp_dir, sync::Arc};
use uuid::Uuid;

#[tokio::test]
async fn test_organizations() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let metadata = Metadata::try_new(store.clone())?;

    let _ = metadata
        .organizations
        .create(CreateRequest {
            name: "organization1".into(),
        })
        .await
        .unwrap();
    let _ = metadata
        .organizations
        .create(CreateRequest {
            name: "organization2".into(),
        })
        .await
        .unwrap();

    let list = metadata
        .organizations
        .list(ListRequest {
            offset: None,
            limit: None,
        })
        .await
        .unwrap();

    dbg!(list);

    // assert_eq!(md.organizations.list_organizations().await?[0].id, 1);
    // assert_eq!(md.organizations.list_organizations().await?[1].id, 2);

    Ok(())
}
