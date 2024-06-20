#![feature(let_chains, concat_idents)]
#![feature(exclusive_range_pattern)]
#![feature(slice_group_by)]

extern crate core;

pub use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
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
use arrow::datatypes::{DataType, Schema};
use arrow::datatypes::IntervalUnit;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow2::array::Int128Array;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_expr::{Extension, LogicalPlan};
use common::query::PropertyRef;
pub use context::Context;
pub use error::Result;
use indexmap::IndexMap;
use tracing::debug;
use metadata::MetadataProvider;
pub use provider::QueryProvider;
use storage::db::OptiDBImpl;
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::physical_plan::planner::QueryPlanner;

pub mod context;
pub mod error;
pub mod expr;
pub mod logical_plan;
pub mod physical_plan;
pub mod provider;
pub mod queries;
pub mod event_records;

pub const DEFAULT_BATCH_SIZE: usize = 4096;

#[macro_export]
macro_rules! breakdowns_to_dicts {
    ($md:expr, $ctx:expr,$breakdowns:expr, $cols_hash:expr,$decode_cols:expr) => {{
        for breakdown in $breakdowns.iter() {
            match &breakdown {
                Breakdown::Property(prop) => {
                    let p = match prop {
                        PropertyRef::System(name) => $md
                            .system_properties
                            .get_by_name($ctx.project_id, name.as_str())?,
                        PropertyRef::Group(name, group) => $md.group_properties[*group]
                            .get_by_name($ctx.project_id, name.as_str())?,
                        PropertyRef::Event(name) => $md
                            .event_properties
                            .get_by_name($ctx.project_id, name.as_str())?,
                        _ => unimplemented!(),
                    };
                    if !p.is_dictionary {
                        continue;
                    }
                    if $cols_hash.contains_key(p.column_name().as_str()) {
                        continue;
                    }

                    let dict = SingleDictionaryProvider::new(
                        $ctx.project_id,
                        p.column_name(),
                        $md.dictionaries.clone(),
                    );
                    let col = Column::from_name(p.column_name());

                    $decode_cols.push((col, Arc::new(dict)));
                    $cols_hash.insert(p.column_name(), ());
                }
            };
        }
    }};
}

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
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub typ: ColumnType,
    pub is_nullable: bool,
    pub data_type: DataType,
    pub hidden: bool,
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

pub async fn initial_plan(
    db: &Arc<OptiDBImpl>,
    projection: Vec<usize>,
) -> Result<(SessionContext, SessionState, LogicalPlan)> {
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::new_with_config_rt(
        SessionConfig::new().with_collect_statistics(true),
        runtime,
    )
        .with_query_planner(Arc::new(QueryPlanner {}));

    let exec_ctx = SessionContext::new_with_state(state.clone());
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(DbParquetNode::try_new(db.clone(), projection.clone())?),
    });

    Ok((exec_ctx, state, plan))
}

pub async fn execute(
    ctx: SessionContext,
    state: SessionState,
    plan: LogicalPlan,
) -> Result<RecordBatch> {
    let physical_plan = state.create_physical_plan(&plan).await?;
    let displayable_plan = DisplayableExecutionPlan::with_full_metrics(physical_plan.as_ref());
    debug!("physical plan: {}", displayable_plan.indent(true));
    let batches = collect(physical_plan, ctx.task_ctx()).await?;
    let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
    let rows_count = batches.iter().fold(0, |acc, x| acc + x.num_rows());
    let res = concat_batches(&schema, &batches, rows_count)?;

    Ok(res)
}

pub mod test_util {

    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::array::RecordBatch;
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use arrow::datatypes::Schema;
    use arrow::datatypes::TimeUnit;
    use common::group_col;
    use common::types::DType;
    use common::types::COLUMN_CREATED_AT;
    use common::types::COLUMN_EVENT;
    use common::types::COLUMN_PROJECT_ID;
    use common::types::TIME_UNIT;
    use common::DECIMAL_PRECISION;
    use common::DECIMAL_SCALE;
    use common::GROUPS_COUNT;
    use common::GROUP_USER_ID;
    use datafusion::datasource::listing::ListingTable;
    use datafusion::datasource::listing::ListingTableConfig;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::datasource::provider_as_source;
    use datafusion::execution::context::SessionState;
    use datafusion::execution::options::ReadOptions;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::CsvReadOptions;
    use datafusion::prelude::SessionConfig;
    use datafusion::prelude::SessionContext;
    use datafusion_common::config::TableOptions;
    use datafusion_expr::logical_plan::builder::UNNAMED_TABLE;
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::LogicalPlanBuilder;
    use metadata::events;
    use metadata::properties;
    use metadata::properties::CreatePropertyRequest;
    use metadata::properties::Property;
    use metadata::properties::Type;
    use metadata::MetadataProvider;
    use storage::db::OptiDBImpl;

    use crate::error::Result;
    use crate::physical_plan::planner::QueryPlanner;

    pub async fn events_provider(db: Arc<OptiDBImpl>, _proj_id: u64) -> Result<LogicalPlan> {
        let group_fields = (0..GROUPS_COUNT)
            .into_iter()
            .map(|gid| Field::new(group_col(gid), DataType::Int64, false))
            .collect::<Vec<_>>();
        let schema = Schema::new(
            vec![
                vec![Field::new(COLUMN_PROJECT_ID, DataType::Int64, false)],
                group_fields,
                vec![
                    Field::new(
                        COLUMN_CREATED_AT,
                        DataType::Timestamp(TIME_UNIT, None),
                        false,
                    ),
                    Field::new(COLUMN_EVENT, DataType::Int64, true),
                    Field::new("user_country", DataType::Int64, true),
                    Field::new("user_device", DataType::Utf8, true),
                    Field::new("user_is_premium", DataType::Boolean, true),
                    Field::new("event_product_name", DataType::Utf8, true),
                    Field::new(
                        "event_revenue",
                        DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
                        true,
                    ),
                ],
            ]
            .concat(),
        );
        let mut options = CsvReadOptions::new();
        options.schema = Some(&schema);
        let table_path = ListingTableUrl::parse(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("resources/test/events.csv")
                .to_str()
                .unwrap(),
        )?;
        let session_config = SessionConfig::new();
        let listing_options = options.to_listing_options(&session_config, TableOptions::new());

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(Arc::new(db.schema1("events")?));
        let provider = ListingTable::try_new(config)?;
        Ok(
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(provider)), None)?
                .build()?,
        )
    }

    pub fn empty_provider() -> Result<LogicalPlan> {
        Ok(LogicalPlanBuilder::empty(false).build()?)
    }

    pub fn create_property(
        md: &Arc<MetadataProvider>,
        _db: &Arc<OptiDBImpl>,
        proj_id: u64,
        req: CreatePropertyRequest,
    ) -> Result<Property> {
        let prop = match req.typ {
            Type::Event => md.event_properties.create(proj_id, req)?,
            Type::Group(gid) => md.group_properties[gid].create(proj_id, req)?,
            _ => unimplemented!(),
        };

        // db.add_field("events", prop.column_name().as_str(), prop.data_type.clone().into(), prop.nullable)?;

        Ok(prop)
    }

    pub async fn create_entities(
        md: Arc<MetadataProvider>,
        db: &Arc<OptiDBImpl>,
        proj_id: u64,
    ) -> Result<()> {
        db.add_field("events", COLUMN_PROJECT_ID, DType::Int64, false)?;
        for gid in 0..GROUPS_COUNT {
            db.add_field("events", group_col(gid).as_str(), DType::Int64, false)?;
        }
        db.add_field("events", COLUMN_CREATED_AT, DType::Timestamp, false)?;
        db.add_field("events", COLUMN_EVENT, DType::Int64, false)?;
        // create user props

        let country_prop = create_property(&md, db, proj_id, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Country".to_string(),
            description: None,
            display_name: None,
            typ: properties::Type::Group(GROUP_USER_ID),
            data_type: DType::String,
            status: properties::Status::Enabled,
            hidden: false,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: true,
            dictionary_type: Some(properties::DictionaryType::Int8),
        })?;

        md.dictionaries
            .get_key_or_create(proj_id, country_prop.column_name().as_str(), "spain")?;
        md.dictionaries.get_key_or_create(
            proj_id,
            country_prop.column_name().as_str(),
            "german",
        )?;

        create_property(&md, db, proj_id, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Device".to_string(),
            description: None,
            display_name: None,
            typ: properties::Type::Group(GROUP_USER_ID),
            data_type: DType::String,
            status: properties::Status::Enabled,
            nullable: true,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
            hidden: false,
        })?;

        create_property(&md, db, proj_id, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Is Premium".to_string(),
            description: None,
            display_name: None,
            typ: Type::Group(GROUP_USER_ID),
            data_type: DType::Boolean,
            status: properties::Status::Enabled,
            hidden: false,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        })?;

        // create events
        md.events.create(proj_id, events::CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "View Product".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            custom_properties: None,
            is_system: false,
            event_properties: None,
            user_properties: None,
        })?;

        md.events.create(proj_id, events::CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "Buy Product".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            custom_properties: None,
            is_system: false,
            event_properties: None,
            user_properties: None,
        })?;

        // create event props
        create_property(&md, db, proj_id, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Product Name".to_string(),
            description: None,
            display_name: None,
            typ: Type::Event,
            data_type: DType::String,
            status: properties::Status::Enabled,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
            hidden: false,
        })?;

        create_property(&md, db, proj_id, CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Revenue".to_string(),
            description: None,
            display_name: None,
            typ: Type::Event,
            data_type: DType::Decimal,
            status: properties::Status::Enabled,
            nullable: true,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
            hidden: false,
        })?;

        Ok(())
    }

    pub async fn run_plan(plan: LogicalPlan) -> Result<Vec<RecordBatch>> {
        let runtime = Arc::new(RuntimeEnv::default());
        let config = SessionConfig::new();
        #[allow(deprecated)]
        let session_state = SessionState::with_config_rt(config, runtime)
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_optimizer_rules(vec![])
            .with_physical_optimizer_rules(vec![]);
        #[allow(deprecated)]
        let exec_ctx = SessionContext::with_state(session_state.clone());
        let physical_plan = session_state.create_physical_plan(&plan).await?;

        Ok(collect(physical_plan, exec_ctx.task_ctx()).await?)
    }
}

pub fn col_name(ctx: &Context, prop: &PropertyRef, md: &Arc<MetadataProvider>) -> Result<String> {
    let name = match prop {
        PropertyRef::System(v) => md
            .system_properties
            .get_by_name(ctx.project_id, v)?
            .column_name(),
        PropertyRef::Group(v, group) => md.group_properties[*group]
            .get_by_name(ctx.project_id, v)?
            .column_name(),
        PropertyRef::Event(v) => md
            .event_properties
            .get_by_name(ctx.project_id, v)?
            .column_name(),
        _ => unimplemented!(),
    };

    Ok(name)
}
