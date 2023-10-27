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
use metadata::database::Column;
use metadata::database::CreateTableRequest;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::error::MetadataError;
use metadata::properties::DataType;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

struct Builders {
    b_user_id: Int64Builder,
    b_created_at: TimestampNanosecondBuilder,
    b_event: UInt64Builder,
    b_i8: Int8Builder,
    b_i16: Int16Builder,
    b_i32: Int32Builder,
    b_i64: Int64Builder,
    b_u8: UInt8Builder,
    b_u16: UInt16Builder,
    b_u32: UInt32Builder,
    b_u64: UInt64Builder,
    b_f64: Float64Builder,
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
            b_user_id: Int64Builder::new(),
            b_created_at: TimestampNanosecondBuilder::new(),
            b_event: UInt64Builder::new(),
            b_i8: Int8Builder::new(),
            b_i16: Int16Builder::new(),
            b_i32: Int32Builder::new(),
            b_i64: Int64Builder::new(),
            b_u8: UInt8Builder::new(),
            b_u16: UInt16Builder::new(),
            b_u32: UInt32Builder::new(),
            b_u64: UInt64Builder::new(),
            b_f64: Float64Builder::new(),
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
                Arc::new(self.b_user_id.finish()) as ArrayRef,
                Arc::new(self.b_created_at.finish()) as ArrayRef,
                Arc::new(self.b_event.finish()) as ArrayRef,
                Arc::new(self.b_i8.finish()) as ArrayRef,
                Arc::new(self.b_i16.finish()) as ArrayRef,
                Arc::new(self.b_i32.finish()) as ArrayRef,
                Arc::new(self.b_i64.finish()) as ArrayRef,
                Arc::new(self.b_u8.finish()) as ArrayRef,
                Arc::new(self.b_u16.finish()) as ArrayRef,
                Arc::new(self.b_u32.finish()) as ArrayRef,
                Arc::new(self.b_u64.finish()) as ArrayRef,
                Arc::new(self.b_f64.finish()) as ArrayRef,
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

pub fn gen(
    partitions: usize,
    batch_size: usize,
    md: &Arc<MetadataProvider>,
    org_id: u64,
    proj_id: u64,
) -> Result<Vec<Vec<RecordBatch>>, anyhow::Error> {
    let mut cols: Vec<Column> = Vec::new();

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "User ID".to_string(),
            typ: Type::Event,
            data_type: DataType::Int64,
            nullable: false,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Created At".to_string(),
            typ: Type::Event,
            data_type: DataType::Timestamp,
            nullable: false,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Event".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: false,
            dict: Some(DictionaryType::UInt64),
        },
        &mut cols,
    )?;

    let props = vec![
        ("i_8", DataType::Int8),
        ("i_16", DataType::Int16),
        ("i_32", DataType::Int32),
        ("i_64", DataType::Int64),
        ("u_8", DataType::UInt8),
        ("u_16", DataType::UInt16),
        ("u_32", DataType::UInt32),
        ("u_64", DataType::UInt64),
        ("f_64", DataType::Float64),
        ("bool", DataType::Boolean),
        ("string", DataType::String),
        ("timestamp", DataType::Timestamp),
        ("decimal", DataType::Decimal),
        ("group", DataType::Int64),
        ("v", DataType::Int64),
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
            &mut cols,
        )?;
    }

    create_event(md, org_id, proj_id, "event".to_string())?;
    let table = CreateTableRequest {
        typ: TableRef::Events(org_id, proj_id),
        columns: cols.clone(),
    };
    md.dictionaries
        .get_key_or_create(1, 1, "event_event", "event")?;

    match md.database.create_table(table.clone()) {
        Ok(_) | Err(MetadataError::AlreadyExists(_)) => {}
        Err(err) => return Err(err.into()),
    };

    let now = NaiveDateTime::from_timestamp_opt(Utc::now().timestamp(), 0)
        .unwrap()
        .duration_trunc(Duration::days(1))?;
    let table = Table {
        id: 1,
        typ: TableRef::Events(org_id, proj_id),
        columns: cols,
    };

    let schema = table.arrow_schema();

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
                builders[partition].b_user_id.append_value(user as i64);
                builders[partition]
                    .b_created_at
                    .append_value(event_time.timestamp_nanos());
                builders[partition].b_event.append_value(1);
                builders[partition].b_i8.append_value(event as i8);
                builders[partition].b_i16.append_value(event as i16);
                builders[partition].b_i32.append_value(event as i32);
                builders[partition].b_i64.append_value(event);
                builders[partition].b_u8.append_value(event as u8);
                builders[partition].b_u16.append_value(event as u16);
                builders[partition].b_u32.append_value(event as u32);
                builders[partition].b_u64.append_value(event as u64);
                builders[partition].b_f64.append_value(event as f64 * 1.1);
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
                    let batch = builders[partition].finish(Arc::new(schema.clone()));
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
            let batch = builders[partition].finish(Arc::new(schema.clone()));
            batches.push(batch);
        }
    }

    Ok(res)
}
