use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::Array;
use arrow::array::ArrayAccessor;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use common::group_col;
use common::event_segmentation::EventSegmentation;
use common::event_segmentation::Query;
use common::funnel::Funnel;
use common::query::Breakdown;
use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::types::{COLUMN_CREATED_AT, ROUND_DIGITS};
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::GROUP_COLUMN_CREATED_AT;
use common::types::GROUP_COLUMN_ID;
use common::types::GROUP_COLUMN_PROJECT_ID;
use common::types::TABLE_EVENTS;
use common::DECIMAL_SCALE;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::basic::ConvertedType::NONE;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::Extension;
use datafusion_expr::LogicalPlan;
use num_traits::ToPrimitive;
use metadata::MetadataProvider;
use rust_decimal::Decimal;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::{col_name, execute, initial_plan};
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::physical_plan::planner::QueryPlanner;
use crate::queries::group_records_search;
use crate::queries::group_records_search::GroupRecordsSearch;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::Column;
use crate::ColumnType;
use crate::Context;
use crate::DataTable;
use crate::error::QueryError;
use crate::Result;

pub struct QueryProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl QueryProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }
}

impl QueryProvider {

    pub async fn group_records_search(
        &self,
        ctx: Context,
        req: GroupRecordsSearch,
    ) -> Result<DataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = if req.properties.is_some() {
            let projection = group_records_search_projection(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };

        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = group_records_search::build(ctx, self.metadata.clone(), plan, req.clone())?;

        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| Column {
                name: field.name().to_owned(),
                typ: ColumnType::Dimension,
                is_nullable: field.is_nullable(),
                data_type: field.data_type().to_owned(),
                hidden: false,
                data: result.column(idx).to_owned(),
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }


}

fn group_records_search_projection(
    ctx: &Context,
    req: &GroupRecordsSearch,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        GROUP_COLUMN_PROJECT_ID.to_string(),
        GROUP_COLUMN_ID.to_string(),
        GROUP_COLUMN_CREATED_AT.to_string(),
    ];
    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                PropValueFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
            }
        }
    }

    for prop in req.properties.clone().unwrap() {
        fields.push(col_name(ctx, &prop, md)?);
    }

    if let Some((prop, ..)) = &req.sort {
        fields.push(col_name(ctx, &prop, md)?);
    }
    Ok(fields)
}
