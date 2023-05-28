#![feature(let_chains, concat_idents)]

extern crate core;

use static_array::array::{Array, BooleanArray, PrimitiveArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use static_array::datatypes::{IntervalUnit, TimeUnit};
use static_array::datatypes::SchemaRef;
use static_array::concatenate;
use async_trait::async_trait;
use datafusion_common::ScalarValue;
use common::query::event_segmentation::EventSegmentation;
pub use context::Context;
pub use error::Result;
pub use provider_impl::ProviderImpl;
pub use std::cmp::Ordering;
use arrow2::datatypes::DataType;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use static_array::array::Int128Array;
use crate::queries::property_values::PropertyValues;

pub mod context;
pub mod error;
pub mod expr;
pub mod logical_plan;
pub mod physical_plan;
pub mod provider_impl;
pub mod queries;
mod static_array;

pub mod event_fields {
    pub const EVENT: &str = "event_event";
    pub const CREATED_AT: &str = "event_created_at";
    pub const USER_ID: &str = "event_user_id";
}

pub const DEFAULT_BATCH_SIZE: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Dimension,
    Metric,
    MetricValue,
    FunnelMetricValue,
}

#[async_trait]
pub trait Provider: Sync + Send {
    async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef>;
    async fn event_segmentation(&self, ctx: Context, es: EventSegmentation) -> Result<DataTable>;
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub typ: ColumnType,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub data: ArrayRef,
}

pub struct DataTable {
    pub schema: SchemaRef,
    pub columns: Vec<Column>,
}

impl DataTable {
    pub fn new(schema: SchemaRef, columns: Vec<Column>) -> Self {
        Self { schema, columns }
    }
}


macro_rules! make_one_col_spans {
    ($spans_state:expr, $arr:expr,$arr_type:ident,$_type:ident, $scalar_type:ident) => {{
        $spans_state.spans.clear();

        let arr = $arr.as_any().downcast_ref::<$arr_type>().unwrap();
        let mut last_value: Option<$_type> = if $spans_state.first {
            $spans_state.first = false;
            match arr.is_null(0) {
                true => None,
                false => Some(arr.value(0)),
            }
        } else {
            match $spans_state.last_partition_value {
                ScalarValue::$scalar_type(v) => v,
                _ => unreachable!(),
            }
        };

        for v in arr.iter() {
            match last_value.partial_cmp(&v) {
                Some(ord) => match ord {
                    Ordering::Less | Ordering::Greater => {
                        $spans_state.spans.push(true);
                        last_value = v.clone();
                    }
                    Ordering::Equal => $spans_state.spans.push(false),
                },
                _ => unreachable!(),
            };
        }

        $spans_state.last_partition_value = ScalarValue::$scalar_type(last_value);
    }};
}

pub struct OneColSpansState {
    pub first: bool,
    pub last_partition_value: ScalarValue,
    pub spans: Vec<bool>,
}

impl OneColSpansState {
    pub fn new(len: usize) -> Self {
        Self {
            first: true,
            last_partition_value: ScalarValue::Null,
            spans: Vec::with_capacity(len),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, Int64Array, PrimitiveArray};
    use crate::OneColSpansState;
    use datafusion_common::ScalarValue;
    pub use std::cmp::Ordering;
    use std::sync::Arc;
    use arrow::datatypes::Int64Type;

    #[test]
    fn test_make_one_col_spans() {
        let arr = Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 2])) as ArrayRef;
        let mut state = OneColSpansState::new(arr.len());

        make_one_col_spans!(state, arr, Int64Array,i64,Int64);

        assert_eq!(state.last_partition_value, ScalarValue::Int64(Some(2)));
        assert_eq!(state.spans, vec![false, false, false, false, true, false]);

        let arr = Arc::new(Int64Array::from(vec![2, 2, 2, 2, 3])) as ArrayRef;
        make_one_col_spans!(state, arr, Int64Array,i64,Int64);

        assert_eq!(state.last_partition_value, ScalarValue::Int64(Some(3)));
        assert_eq!(state.spans, vec![false, false, false, false, true]);

        let arr = Arc::new(Int64Array::from(vec![4, 4])) as ArrayRef;
        make_one_col_spans!(state, arr, Int64Array,i64,Int64);

        assert_eq!(state.last_partition_value, ScalarValue::Int64(Some(4)));
        assert_eq!(state.spans, vec![true, false]);

        let arr = Arc::new(Int64Array::from(vec![5])) as ArrayRef;
        make_one_col_spans!(state, arr, Int64Array,i64,Int64);

        assert_eq!(state.last_partition_value, ScalarValue::Int64(Some(5)));
        assert_eq!(state.spans, vec![true]);
    }
}

pub mod test_util {
    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use datafusion::datasource::listing::ListingTable;
    use datafusion::datasource::listing::ListingTableConfig;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::datasource::provider_as_source;
    use datafusion::execution::options::ReadOptions;
    use datafusion::prelude::CsvReadOptions;
    use datafusion::prelude::SessionConfig;
    use datafusion_expr::logical_plan::builder::UNNAMED_TABLE;
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::LogicalPlanBuilder;
    use metadata::database;
    use metadata::database::Column;
    use metadata::database::Table;
    use metadata::database::TableRef;
    use metadata::events;
    use metadata::properties;
    use metadata::properties::provider_impl::Namespace;
    use metadata::properties::CreatePropertyRequest;
    use metadata::properties::Property;
    use metadata::store::Store;
    use metadata::MetadataProvider;
    use uuid::Uuid;

    use crate::error::Result;
    use crate::event_fields;

    pub async fn events_provider(
        db: Arc<dyn database::Provider>,
        org_id: u64,
        proj_id: u64,
    ) -> Result<LogicalPlan> {
        let table = db.get_table(TableRef::Events(org_id, proj_id)).await?;
        let schema = table.arrow_schema();

        let options = CsvReadOptions::new();
        let table_path = ListingTableUrl::parse(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("resources/test/events.csv")
                .to_str()
                .unwrap(),
        )?;
        let target_partitions = 1;
        let session_config = SessionConfig::new().with_target_partitions(target_partitions);
        let listing_options = options.to_listing_options(&session_config);

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(Arc::new(schema));
        let provider = ListingTable::try_new(config)?;
        Ok(
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(provider)), None)?
                .build()?,
        )
    }

    pub fn empty_provider() -> Result<LogicalPlan> {
        Ok(LogicalPlanBuilder::empty(false).build()?)
    }

    pub async fn create_property(
        md: &Arc<MetadataProvider>,
        ns: Namespace,
        org_id: u64,
        proj_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let prop = match ns {
            Namespace::Event => md.event_properties.create(org_id, proj_id, req).await?,
            Namespace::User => md.user_properties.create(org_id, proj_id, req).await?,
        };

        md.database
            .add_column(
                TableRef::Events(org_id, proj_id),
                Column::new(
                    prop.column_name(ns),
                    prop.typ.clone(),
                    prop.nullable,
                    prop.dictionary_type.clone(),
                ),
            )
            .await?;

        Ok(prop)
    }

    pub async fn create_entities(
        md: Arc<MetadataProvider>,
        org_id: u64,
        proj_id: u64,
    ) -> Result<()> {
        md.database
            .create_table(Table {
                typ: TableRef::Events(org_id, proj_id),
                columns: vec![],
            })
            .await?;

        md.database
            .add_column(
                TableRef::Events(org_id, proj_id),
                Column::new(
                    event_fields::USER_ID.to_string(),
                    DataType::UInt64,
                    false,
                    None,
                ),
            )
            .await?;
        md.database
            .add_column(
                TableRef::Events(org_id, proj_id),
                Column::new(
                    event_fields::CREATED_AT.to_string(),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                    false,
                    None,
                ),
            )
            .await?;
        md.database
            .add_column(
                TableRef::Events(org_id, proj_id),
                Column::new(
                    event_fields::EVENT.to_string(),
                    DataType::UInt16,
                    false,
                    None,
                ),
            )
            .await?;

        // create user props

        let country_prop = create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                tags: None,
                name: "Country".to_string(),
                description: None,
                display_name: None,
                typ: DataType::Utf8,
                status: properties::Status::Enabled,
                is_system: false,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: Some(DataType::UInt8),
            },
        )
        .await?;

        md.dictionaries
            .get_key_or_create(
                org_id,
                proj_id,
                country_prop.column_name(Namespace::User).as_str(),
                "spain",
            )
            .await?;
        md.dictionaries
            .get_key_or_create(
                org_id,
                proj_id,
                country_prop.column_name(Namespace::User).as_str(),
                "german",
            )
            .await?;
        create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                tags: None,
                name: "Device".to_string(),
                description: None,
                display_name: None,
                typ: DataType::Utf8,
                status: properties::Status::Enabled,
                nullable: true,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
                is_system: false,
            },
        )
        .await?;

        create_property(
            &md,
            Namespace::User,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                tags: None,
                name: "Is Premium".to_string(),
                description: None,
                display_name: None,
                typ: DataType::Boolean,
                status: properties::Status::Enabled,
                is_system: false,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
        .await?;

        // create events
        md.events
            .create(org_id, proj_id, events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "View Product".to_string(),
                display_name: None,
                description: None,
                status: events::Status::Enabled,
                properties: None,
                custom_properties: None,
                is_system: false,
            })
            .await?;

        md.events
            .create(org_id, proj_id, events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "Buy Product".to_string(),
                display_name: None,
                description: None,
                status: events::Status::Enabled,
                properties: None,
                custom_properties: None,
                is_system: false,
            })
            .await?;

        // create event props
        create_property(
            &md,
            Namespace::Event,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                tags: None,
                name: "Product Name".to_string(),
                description: None,
                display_name: None,
                typ: DataType::Utf8,
                status: properties::Status::Enabled,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
                is_system: false,
            },
        )
        .await?;

        create_property(
            &md,
            Namespace::Event,
            org_id,
            proj_id,
            CreatePropertyRequest {
                created_by: 0,
                tags: None,
                name: "Revenue".to_string(),
                description: None,
                display_name: None,
                typ: DataType::Float64,
                status: properties::Status::Enabled,
                nullable: true,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
                is_system: false,
            },
        )
        .await?;

        Ok(())
    }

    pub fn create_md() -> Result<Arc<MetadataProvider>> {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));

        let store = Arc::new(Store::new(path));
        Ok(Arc::new(MetadataProvider::try_new(store)?))
    }
}
