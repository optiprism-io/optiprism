#![feature(let_chains, concat_idents)]
#![feature(exclusive_range_pattern)]
#![feature(slice_group_by)]

extern crate core;

pub use std::cmp::Ordering;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::BooleanArray;
use arrow::array::Date32Array;
use arrow::array::Date64Array;
use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::array::DurationMicrosecondArray;
use arrow::array::DurationMillisecondArray;
use arrow::array::DurationNanosecondArray;
use arrow::array::DurationSecondArray;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::Float16Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::IntervalDayTimeArray;
use arrow::array::IntervalMonthDayNanoArray;
use arrow::array::IntervalYearMonthArray;
use arrow::array::LargeBinaryArray;
use arrow::array::LargeStringArray;
use arrow::array::StringArray;
use arrow::array::Time32MillisecondArray;
use arrow::array::Time32SecondArray;
use arrow::array::Time64MicrosecondArray;
use arrow::array::Time64NanosecondArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::TimestampMillisecondArray;
use arrow::array::TimestampNanosecondArray;
use arrow::array::TimestampSecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::datatypes::DataType;
use arrow::datatypes::IntervalUnit;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow2::array::Int128Array;
use async_trait::async_trait;
use common::query::event_segmentation::EventSegmentation;
pub use context::Context;
pub use error::Result;
pub use provider_impl::ProviderImpl;

use crate::queries::property_values::PropertyValues;

pub mod context;
pub mod error;
pub mod expr;
pub mod logical_plan;
pub mod physical_plan;
pub mod provider_impl;
pub mod queries;

pub mod event_fields {
    pub const EVENT: &str = "event_event";
    pub const CREATED_AT: &str = "event_created_at";
    pub const USER_ID: &str = "event_user_id";
}

pub const DEFAULT_BATCH_SIZE: usize = 4096;

macro_rules! static_array_enum {
    ($($ident:ident)+) => {
        #[derive(Debug,Clone)]
        pub enum StaticArray {
            $($ident(concat_idents!($ident,Array)),)+
        }
    }
}

static_array_enum! {
    Int8 Int16 Int32 Int64 Int128 UInt8 UInt16 UInt32 UInt64 Float16 Float32 Float64 Boolean
    TimestampNanosecond TimestampMicrosecond TimestampMillisecond  Time32Second Time32Millisecond  Time64Microsecond Time64Nanosecond DurationSecond DurationMillisecond DurationMicrosecond IntervalYearMonth IntervalDayTime IntervalMonthDayNano Date32 Date64 DurationNanosecond
    TimestampSecond FixedSizeBinary Binary LargeBinary String LargeString Decimal128 Decimal256
}

macro_rules! impl_into_static_array {
    ($($ident:ident)+) => {
        impl StaticArray {
    fn eq_values(&self, left_row_id: usize, right:&StaticArray,right_row_id: usize) -> bool {
        match (self,right) {
             $((StaticArray::$ident(a),StaticArray::$ident(b))=>a.value(left_row_id) == b.value(right_row_id)),+,
            _=>unreachable!()
        }
        }
    }
    }
}

impl_into_static_array!( Int8 Int16 Int32 Int64 Int128 UInt8 UInt16 UInt32 UInt64 Float16 Float32 Float64 Boolean
    TimestampNanosecond TimestampMicrosecond TimestampMillisecond  Time32Second Time32Millisecond  Time64Microsecond Time64Nanosecond DurationSecond DurationMillisecond DurationMicrosecond IntervalYearMonth IntervalDayTime IntervalMonthDayNano Date32 Date64 DurationNanosecond
    TimestampSecond FixedSizeBinary Binary LargeBinary String LargeString);

impl From<ArrayRef> for StaticArray {
    fn from(arr: ArrayRef) -> Self {
        match arr.data_type() {
            DataType::Boolean => {
                StaticArray::Boolean(arr.as_any().downcast_ref::<BooleanArray>().unwrap().clone())
            }
            DataType::Int8 => {
                StaticArray::Int8(arr.as_any().downcast_ref::<Int8Array>().unwrap().clone())
            }
            DataType::Int16 => {
                StaticArray::Int16(arr.as_any().downcast_ref::<Int16Array>().unwrap().clone())
            }
            DataType::Int32 => {
                StaticArray::Int32(arr.as_any().downcast_ref::<Int32Array>().unwrap().clone())
            }
            DataType::Int64 => {
                StaticArray::Int64(arr.as_any().downcast_ref::<Int64Array>().unwrap().clone())
            }
            DataType::UInt8 => {
                StaticArray::UInt8(arr.as_any().downcast_ref::<UInt8Array>().unwrap().clone())
            }
            DataType::UInt16 => {
                StaticArray::UInt16(arr.as_any().downcast_ref::<UInt16Array>().unwrap().clone())
            }
            DataType::UInt32 => {
                StaticArray::UInt32(arr.as_any().downcast_ref::<UInt32Array>().unwrap().clone())
            }
            DataType::UInt64 => {
                StaticArray::UInt64(arr.as_any().downcast_ref::<UInt64Array>().unwrap().clone())
            }
            DataType::Float16 => {
                StaticArray::Float16(arr.as_any().downcast_ref::<Float16Array>().unwrap().clone())
            }
            DataType::Float32 => {
                StaticArray::Float32(arr.as_any().downcast_ref::<Float32Array>().unwrap().clone())
            }
            DataType::Float64 => {
                StaticArray::Float64(arr.as_any().downcast_ref::<Float64Array>().unwrap().clone())
            }
            DataType::Timestamp(tu, _tz) => match tu {
                TimeUnit::Second => StaticArray::TimestampSecond(
                    arr.as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Millisecond => StaticArray::TimestampMillisecond(
                    arr.as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Microsecond => StaticArray::TimestampMicrosecond(
                    arr.as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Nanosecond => StaticArray::TimestampNanosecond(
                    arr.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap()
                        .clone(),
                ),
            },
            DataType::Date32 => {
                StaticArray::Date32(arr.as_any().downcast_ref::<Date32Array>().unwrap().clone())
            }
            DataType::Date64 => {
                StaticArray::Date64(arr.as_any().downcast_ref::<Date64Array>().unwrap().clone())
            }
            DataType::Time32(tu) => match tu {
                TimeUnit::Second => StaticArray::Time32Second(
                    arr.as_any()
                        .downcast_ref::<Time32SecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Millisecond => StaticArray::Time32Millisecond(
                    arr.as_any()
                        .downcast_ref::<Time32MillisecondArray>()
                        .unwrap()
                        .clone(),
                ),
                _ => unimplemented!(),
            },
            DataType::Time64(tu) => match tu {
                TimeUnit::Microsecond => StaticArray::Time64Microsecond(
                    arr.as_any()
                        .downcast_ref::<Time64MicrosecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Nanosecond => StaticArray::Time64Nanosecond(
                    arr.as_any()
                        .downcast_ref::<Time64NanosecondArray>()
                        .unwrap()
                        .clone(),
                ),
                _ => unimplemented!(),
            },
            DataType::Duration(tu) => match tu {
                TimeUnit::Second => StaticArray::DurationSecond(
                    arr.as_any()
                        .downcast_ref::<DurationSecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Millisecond => StaticArray::DurationMillisecond(
                    arr.as_any()
                        .downcast_ref::<DurationMillisecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Microsecond => StaticArray::DurationMicrosecond(
                    arr.as_any()
                        .downcast_ref::<DurationMicrosecondArray>()
                        .unwrap()
                        .clone(),
                ),
                TimeUnit::Nanosecond => StaticArray::DurationNanosecond(
                    arr.as_any()
                        .downcast_ref::<DurationNanosecondArray>()
                        .unwrap()
                        .clone(),
                ),
            },
            DataType::Interval(i) => match i {
                IntervalUnit::YearMonth => StaticArray::IntervalYearMonth(
                    arr.as_any()
                        .downcast_ref::<IntervalYearMonthArray>()
                        .unwrap()
                        .clone(),
                ),
                IntervalUnit::DayTime => StaticArray::IntervalDayTime(
                    arr.as_any()
                        .downcast_ref::<IntervalDayTimeArray>()
                        .unwrap()
                        .clone(),
                ),
                IntervalUnit::MonthDayNano => StaticArray::IntervalMonthDayNano(
                    arr.as_any()
                        .downcast_ref::<IntervalMonthDayNanoArray>()
                        .unwrap()
                        .clone(),
                ),
            },
            DataType::Binary => {
                StaticArray::Binary(arr.as_any().downcast_ref::<BinaryArray>().unwrap().clone())
            }
            DataType::FixedSizeBinary(_) => StaticArray::FixedSizeBinary(
                arr.as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .clone(),
            ),
            DataType::LargeBinary => StaticArray::LargeBinary(
                arr.as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .clone(),
            ),
            DataType::Utf8 => {
                StaticArray::String(arr.as_any().downcast_ref::<StringArray>().unwrap().clone())
            }
            DataType::LargeUtf8 => StaticArray::LargeString(
                arr.as_any()
                    .downcast_ref::<LargeStringArray>()
                    .unwrap()
                    .clone(),
            ),
            DataType::Decimal128(_, _) => StaticArray::Decimal128(
                arr.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .clone(),
            ),
            DataType::Decimal256(_, _) => StaticArray::Decimal256(
                arr.as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .clone(),
            ),
            _ => unimplemented!(),
        }
    }
}

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

pub mod test_util {
    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use common::DECIMAL_PRECISION;
    use common::DECIMAL_SCALE;
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
                    DataType::Int64,
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
                typ: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
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
