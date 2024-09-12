use std::env::temp_dir;
use std::sync::Arc;

use common::event_segmentation::Analysis;
use common::event_segmentation::ChartType;
use common::event_segmentation::EventSegmentationRequest;
use common::query::QueryTime;
use common::query::TimeIntervalUnit;
use metadata::bookmarks::Bookmarks;
use metadata::bookmarks::CreateBookmarkRequest;
use metadata::error::Result;
use metadata::reports::Query;
use uuid::Uuid;
#[test]
fn test_reports() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let bookmarks: Box<Bookmarks> = Box::new(Bookmarks::new(db.clone()));

    let create_bookmark_req = CreateBookmarkRequest {
        created_by: 1,
        query: Some(Query::EventSegmentation(EventSegmentationRequest {
            time: QueryTime::Last {
                last: 1,
                unit: TimeIntervalUnit::Day,
            },
            group_id: 1,
            interval_unit: TimeIntervalUnit::Day,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![],
            filters: None,
            breakdowns: None,
            segments: None,
        })),
    };
    let b = bookmarks.create(1, create_bookmark_req)?;
    dbg!(&b);
    let bb = bookmarks.get_by_id(1, 1, &b.id)?;
    dbg!(bb);
    Ok(())
}
