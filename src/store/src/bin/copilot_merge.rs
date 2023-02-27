use futures::Stream;
use parquet2::page::CompressedPage;
use parquet2::read::get_page_iterator;
use store::error::Result;

/*
type describes a vector, where vector index is a column index, and vector value is a compressed page
 */
type IndexColumnsArrayRow = Vec<Vec<i64>>;

/*
 Takes a vector of streams, where each stream is a stream of sorted arrays
 Streams are continuous
 Returns a merged stream
 */
fn merge_streams(streams: Vec<Box<dyn Stream<Item=IndexColumnsArrayRow>>>) -> Result<Box<dyn Stream<Item=IndexColumnsArrayRow>>> {
let me

}

fn main() {
    let pages = get_page_iterator(column_metadata, &mut reader, None, vec![], 1024 * 1024)?;
}