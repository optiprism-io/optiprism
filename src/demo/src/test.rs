use std::ops::Add;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::ArrayRef;
use arrow::array::BooleanBuilder;
use arrow::array::Decimal128Builder;
use arrow::array::Float32Builder;
use arrow::array::Float64Builder;
use arrow::array::Int16Builder;
use arrow::array::Int32Builder;
use arrow::array::Int64Builder;
use arrow::array::Int8Builder;
use arrow::array::LargeStringBuilder;
use arrow::array::StringBuilder;
use arrow::array::TimestampNanosecondBuilder;
use arrow::array::TimestampSecondBuilder;
use arrow::array::UInt16Builder;
use arrow::array::UInt32Builder;
use arrow::array::UInt64Builder;
use arrow::array::UInt8Builder;
use arrow::datatypes::SchemaRef;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use common::types::{COLUMN_EVENT, COLUMN_PROJECT_ID, COLUMN_CREATED_AT, COLUMN_USER_ID, DType};
use metadata::error::MetadataError;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use store::db::{OptiDBImpl, TableOptions};
use store::{NamedValue, Value};
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

struct Builders {
    b_project_id: Int64Builder,
    b_user_id: Int64Builder,
    b_created_at: TimestampNanosecondBuilder,
    b_event: Int64Builder,
    b_i8: Int8Builder,
    b_i16: Int16Builder,
    b_i32: Int32Builder,
    b_i64: Int64Builder,
    b_b: BooleanBuilder,
    b_str: StringBuilder,
    b_ts: TimestampNanosecondBuilder,
    b_dec: Decimal128Builder,
    b_group: Int64Builder,
    b_v: Int64Builder,
}

impl Builders {
    pub fn new() -> Self {
        Self {
            b_project_id: Int64Builder::new(),
            b_user_id: Int64Builder::new(),
            b_created_at: TimestampNanosecondBuilder::new(),
            b_event: Int64Builder::new(),
            b_i8: Int8Builder::new(),
            b_i16: Int16Builder::new(),
            b_i32: Int32Builder::new(),
            b_i64: Int64Builder::new(),
            b_b: BooleanBuilder::new(),
            b_str: StringBuilder::new(),
            b_ts: TimestampNanosecondBuilder::new(),
            b_dec: Decimal128Builder::new()
                .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
                .unwrap(),
            b_group: Int64Builder::new(),
            b_v: Int64Builder::new(),
        }
    }

    pub fn finish(&mut self, schema: SchemaRef) -> RecordBatch {
        let arrs = {
            vec![
                Arc::new(self.b_project_id.finish()) as ArrayRef,
                Arc::new(self.b_user_id.finish()) as ArrayRef,
                Arc::new(self.b_created_at.finish()) as ArrayRef,
                Arc::new(self.b_event.finish()) as ArrayRef,
                Arc::new(self.b_i8.finish()) as ArrayRef,
                Arc::new(self.b_i16.finish()) as ArrayRef,
                Arc::new(self.b_i32.finish()) as ArrayRef,
                Arc::new(self.b_i64.finish()) as ArrayRef,
                Arc::new(self.b_b.finish()) as ArrayRef,
                Arc::new(self.b_str.finish()) as ArrayRef,
                Arc::new(self.b_ts.finish()) as ArrayRef,
                Arc::new(self.b_dec.finish()) as ArrayRef,
                Arc::new(self.b_group.finish()) as ArrayRef,
                Arc::new(self.b_v.finish()) as ArrayRef,
            ]
        };
        RecordBatch::try_new(schema, arrs).unwrap()
    }

    pub fn len(&self) -> usize {
        self.b_user_id.len()
    }
}

pub fn init(
    partitions: usize,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    org_id: u64,
    proj_id: u64,
) -> Result<(), anyhow::Error> {
    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        partitions,
        index_cols: 2,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024 * 100,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024*8*8,
    };
    db.create_table("events", topts)?;
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_PROJECT_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_USER_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_CREATED_AT.to_string(),
            typ: Type::Event,
            data_type: DType::Timestamp,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_EVENT.to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: false,
            dict: Some(DictionaryType::Int64),
        },
        &db,
    )?;

    let props = vec![
        ("i_8", DType::Int8),
        ("i_16", DType::Int16),
        ("i_32", DType::Int32),
        ("i_64", DType::Int64),
        ("bool", DType::Boolean),
        ("string", DType::String),
        ("timestamp", DType::Timestamp),
        ("decimal", DType::Decimal),
        ("group", DType::Int64),
        ("v", DType::Int64),
    ];

    for (name, dt) in props {
        create_property(
            md,
            org_id,
            proj_id,
            CreatePropertyMainRequest {
                name: name.to_string(),
                typ: Type::Event,
                data_type: dt,
                nullable: true,
                dict: None,
            },
            &db,
        )?;
    }

    create_event(md, org_id, proj_id, "event".to_string())?;
    md.dictionaries
        .get_key_or_create(1, 1, "event_event", "event")?;

    Ok(())
}

pub fn gen_mem(partitions: usize,
           batch_size: usize,
           db: &Arc<OptiDBImpl>,
           proj_id: u64) -> Result<Vec<Vec<RecordBatch>>, anyhow::Error> {
    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;
    let mut res = vec![Vec::new(); partitions];

    let mut builders = Vec::new();
    for _p in 0..partitions {
        builders.push(Builders::new());
    }
    let users = 100;
    let days = 60;
    let events = 10;
    for user in 0..users {
        let partition = user % partitions;
        let mut cur_time = now - Duration::days(days);
        for _day in 0..days {
            let mut event_time = cur_time;
            for event in 0..events {
                builders[partition].b_project_id.append_value(1);
                builders[partition].b_user_id.append_value(user as i64);
                builders[partition]
                    .b_created_at
                    .append_value(event_time.timestamp_nanos());
                builders[partition].b_event.append_value(1);
                builders[partition].b_i8.append_value(event as i8);
                builders[partition].b_i16.append_value(event as i16);
                builders[partition].b_i32.append_value(event as i32);
                builders[partition].b_i64.append_value(event);
                builders[partition].b_b.append_value(event % 2 == 0);
                builders[partition]
                    .b_str
                    .append_value(format!("event {}", event).as_str());
                builders[partition].b_ts.append_value(event * 1000);
                builders[partition].b_dec.append_value(
                    event as i128 * 10_i128.pow(DECIMAL_SCALE as u32) + event as i128 * 100,
                );
                builders[partition]
                    .b_group
                    .append_value(event % (events / 2));
                // two group of users with different "v" value to proper integration tests
                if user % 2 == 0 {
                    builders[partition].b_v.append_value(event);
                } else {
                    builders[partition].b_v.append_value(event * 2);
                }

                if builders[partition].len() >= batch_size {
                    let batch = builders[partition].finish(Arc::new(db.schema1("events")?.clone()));
                    res[partition].push(batch);
                }

                let d = Duration::days(1).num_seconds() / events;
                let diff = Duration::seconds(d);
                event_time = event_time.add(diff);
            }
            cur_time = cur_time.add(Duration::days(1));
        }
    }

    for (partition, batches) in res.iter_mut().enumerate() {
        if builders[partition].len() > 0 {
            let batch = builders[partition].finish(Arc::new(db.schema1("events")?.clone()));
            batches.push(batch);
        }
    }

    Ok(res)
}

pub fn gen(
    db: &Arc<OptiDBImpl>,
    proj_id: u64) -> Result<(), anyhow::Error> {
    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;

    let users = 100;
    let days = 360;
    let events = 10;
    let mut vals: Vec<NamedValue> = vec![];
    for user in 0..users {
        let mut cur_time = now - Duration::days(days);
        for _day in 0..days {
            let mut event_time = cur_time;
            for event in 0..events {
                vals.truncate(0);
                vals.push(NamedValue::new("event_project_id".to_string(), Value::Int64(Some(proj_id as i64))));
                vals.push(NamedValue::new("event_user_id".to_string(), Value::Int64(Some(user as i64+1))));
                vals.push(NamedValue::new("event_created_at".to_string(), Value::Int64(Some(event_time.timestamp_nanos()))));
                vals.push(NamedValue::new("event_event".to_string(), Value::Int64(Some(1))));
                vals.push(NamedValue::new("event_i_8".to_string(), Value::Int8(Some(event as i8))));
                vals.push(NamedValue::new("event_i_16".to_string(), Value::Int16(Some(event as i16))));
                vals.push(NamedValue::new("event_i_32".to_string(), Value::Int32(Some(event as i32))));
                vals.push(NamedValue::new("event_i_64".to_string(), Value::Int64(Some(event))));
                vals.push(NamedValue::new("event_bool".to_string(), Value::Boolean(Some(event % 2 == 0))));
                vals.push(NamedValue::new("event_string".to_string(), Value::String(Some(format!("event {}", event)))));
                vals.push(NamedValue::new("event_timestamp".to_string(), Value::Int64(Some(event * 1000))));
                vals.push(NamedValue::new("event_decimal".to_string(), Value::Decimal(Some(event as i128 * 10_i128.pow(DECIMAL_SCALE as u32) + event as i128 * 100))));
                vals.push(NamedValue::new("event_group".to_string(), Value::Int64(Some(event % (events / 2)))));
                // two group of users with different "v" value to proper integration tests
                if user % 2 == 0 {
                    vals.push(NamedValue::new("event_v".to_string(), Value::Int64(Some(event))));
                } else {
                    vals.push(NamedValue::new("v".to_string(), Value::Int64(Some(event * 2))));
                }

                db.insert("events", vals.clone())?;
                let d = Duration::days(1).num_seconds() / events;
                let diff = Duration::seconds(d);
                event_time = event_time.add(diff);
            }
            cur_time = cur_time.add(Duration::days(1));
        }
    }

    Ok(())
}