use std::fs::File;
use std::time::SystemTime;

use arrow2::error::Error;
use arrow2::io::parquet::read;

fn main() -> Result<(), Error> {
    // say we have a file
    let mut reader = File::open(
        "/opt/homebrew/Caskroom/clickhouse/user_files/store/tables/events/0/4/3.parquet",
    )?;

    // we can read its metadata:
    let metadata = read::read_metadata(&mut reader)?;

    // and infer a [`Schema`] from the `metadata`.
    let schema = read::infer_schema(&metadata)?;

    // we can filter the columns we need (here we select all)
    let schema = schema.filter(|idx, _field| if idx == 1 { true } else { false });

    // we can then read the row groups into chunks
    let chunks = read::FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        Some(1024 * 1024),
        None,
        None,
    );

    let start = SystemTime::now();
    for maybe_chunk in chunks {
        let chunk = maybe_chunk?;
        assert!(!chunk.is_empty());
    }
    println!("took: {:?}", start.elapsed().unwrap());
    Ok(())
}
