use std::env::temp_dir;
use std::sync::Arc;

use common::query::event_segmentation::Analysis;
use common::query::event_segmentation::ChartType;
use common::query::event_segmentation::EventSegmentation;
use common::query::QueryTime;
use common::query::TimeIntervalUnit;
use common::types::OptionalProperty;
use metadata::error::Result;
use metadata::reports::CreateReportRequest;
use metadata::reports::Provider;
use metadata::reports::ProviderImpl;
use metadata::reports::Query;
use metadata::reports::Type;
use metadata::reports::UpdateReportRequest;
use metadata::store::Store;
use uuid::Uuid;
#[test]
fn test_reports() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let reports: Box<dyn Provider> = Box::new(ProviderImpl::new(store.clone()));
    let create_report_req = CreateReportRequest {
        created_by: 0,
        tags: Some(vec![]),
        name: "".to_string(),
        description: None,
        typ: Type::EventSegmentation,
        query: Query::EventSegmentation(EventSegmentation {
            time: QueryTime::Last {
                last: 1,
                unit: TimeIntervalUnit::Second,
            },
            group: "sdf".to_string(),
            interval_unit: TimeIntervalUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![],
            filters: None,
            breakdowns: None,
            segments: None,
        }),
    };

    let update_report_req = UpdateReportRequest {
        updated_by: 1,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        typ: OptionalProperty::Some(Type::EventSegmentation),
        query: OptionalProperty::Some(Query::EventSegmentation(EventSegmentation {
            time: QueryTime::Last {
                last: 1,
                unit: TimeIntervalUnit::Second,
            },
            group: "sdf".to_string(),
            interval_unit: TimeIntervalUnit::Second,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![],
            filters: None,
            breakdowns: None,
            segments: None,
        })),
    };

    // try to get, delete, update unexisting report
    assert!(reports.get_by_id(1, 1, 1).is_err());
    assert!(reports.delete(1, 1, 1).is_err());
    assert!(reports.update(1, 1, 1, update_report_req.clone()).is_err());

    let res = reports.create(1, 1, create_report_req.clone())?;
    assert_eq!(res.id, 1);
    // check existence by id
    assert_eq!(reports.get_by_id(1, 1, 1)?.id, 1);

    reports.update(1, 1, 1, update_report_req.clone())?;
    assert_eq!(reports.list(1, 1)?.data[0].id, 1);

    // delete reports
    assert_eq!(reports.delete(1, 1, 1)?.id, 1);

    // reports should gone now
    assert!(reports.get_by_id(1, 1, 1).is_err());
    Ok(())
}
