use std::ops::Add;
use std::sync::Arc;

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
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;
use chrono::Timelike;
use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use metadata::database::Column;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::error::DatabaseError;
use metadata::properties::provider_impl::Namespace;
use metadata::MetadataProvider;
use rust_decimal::Decimal;
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

pub async fn gen(
    md: &Arc<MetadataProvider>,
    org_id: u64,
    proj_id: u64,
) -> Result<Vec<Vec<RecordBatch>>, anyhow::Error> {
    let mut cols: Vec<Column> = Vec::new();

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "User ID".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Created At".to_string(),
            data_type: DataType::Timestamp(TimeUnit::Nanosecond, None),
            nullable: false,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Event".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            dict: Some(DataType::UInt64),
        },
        &mut cols,
    )
    .await?;

    let props = vec![
        (("i_8", DataType::Int8)),
        (("i_16", DataType::Int16)),
        (("i_32", DataType::Int32)),
        (("i_64", DataType::Int64)),
        (("u_8", DataType::UInt8)),
        (("u_16", DataType::UInt16)),
        (("u_32", DataType::UInt32)),
        (("u_64", DataType::UInt64)),
        (("f_32", DataType::Float32)),
        (("f_64", DataType::Float64)),
        (("bool", DataType::Boolean)),
        (("string", DataType::Utf8)),
        (("timestamp", DataType::Timestamp(TimeUnit::Second, None))),
        (("large_utf_8", DataType::LargeUtf8)),
        ((
            "decimal",
            DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        )),
        (("group", DataType::Int64)),
    ];

    for (name, dt) in props {
        create_property(
            md,
            Namespace::Event,
            org_id,
            proj_id,
            CreatePropertyMainRequest {
                name: name.to_string(),
                data_type: dt,
                nullable: true,
                dict: None,
            },
            &mut cols,
        )
        .await?;
    }

    create_event(md, org_id, proj_id, "event".to_string()).await?;
    let table = Table {
        typ: TableRef::Events(org_id, proj_id),
        columns: cols,
    };
    md.dictionaries
        .get_key_or_create(1, 1, "event_event", "event")
        .await?;

    match md.database.create_table(table.clone()).await {
        Ok(_)
        | Err(metadata::error::MetadataError::Database(DatabaseError::TableAlreadyExists(_))) => {}
        Err(err) => return Err(err.into()),
    };

    let now = NaiveDateTime::from_timestamp(Utc::now().timestamp(), 0)
        .duration_trunc(Duration::days(1))?;
    let schema = table.arrow_schema();
    let mut b_user_id = Int64Builder::new();
    let mut b_created_at = TimestampNanosecondBuilder::new();
    let mut b_event = UInt64Builder::new();
    let mut b_i8 = Int8Builder::new();
    let mut b_i16 = Int16Builder::new();
    let mut b_i32 = Int32Builder::new();
    let mut b_i64 = Int64Builder::new();
    let mut b_u8 = UInt8Builder::new();
    let mut b_u16 = UInt16Builder::new();
    let mut b_u32 = UInt32Builder::new();
    let mut b_u64 = UInt64Builder::new();
    let mut b_f32 = Float32Builder::new();
    let mut b_f64 = Float64Builder::new();
    let mut b_b = BooleanBuilder::new();
    let mut b_str = StringBuilder::new();
    let mut b_ts = TimestampSecondBuilder::new();
    let mut b_lstr = LargeStringBuilder::new();
    let mut b_dec =
        Decimal128Builder::new().with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?;
    let mut b_group = Int64Builder::new();
    let users = 100;
    let days = 100;
    let events = 100;
    for user in 0..users {
        let mut cur_time = now - Duration::days(days);
        for day in 0..days {
            let mut event_time = cur_time.clone();
            for event in 0..events {
                b_user_id.append_value(user);
                b_created_at.append_value(event_time.timestamp_nanos());
                b_event.append_value(1);
                b_i8.append_value(event as i8);
                b_i16.append_value(event as i16);
                b_i32.append_value(event as i32);
                b_i64.append_value(event);
                b_u8.append_value(event as u8);
                b_u16.append_value(event as u16);
                b_u32.append_value(event as u32);
                b_u64.append_value(event as u64);
                b_f32.append_value(event as f32 * 1.1);
                b_f64.append_value(event as f64 * 1.1);
                b_b.append_value(event % 2 == 0);
                b_str.append_value(format!("event {}", event).as_str());
                b_lstr.append_value(format!("event {}", event).as_str());
                b_ts.append_value(event * 1000);
                b_dec.append_value(
                    event as i128 * 10_i128.pow(DECIMAL_SCALE as u32) + event as i128 * 100,
                );
                b_group.append_value(event % (events / 2));
                let d = Duration::days(1).num_seconds() / events;
                let diff = Duration::seconds(d);
                event_time = event_time.add(diff);
            }
            cur_time = cur_time.add(Duration::days(1));
        }
    }
    let cols = {
        vec![
            Arc::new(b_user_id.finish()) as ArrayRef,
            Arc::new(b_created_at.finish()) as ArrayRef,
            Arc::new(b_event.finish()) as ArrayRef,
            Arc::new(b_i8.finish()) as ArrayRef,
            Arc::new(b_i16.finish()) as ArrayRef,
            Arc::new(b_i32.finish()) as ArrayRef,
            Arc::new(b_i64.finish()) as ArrayRef,
            Arc::new(b_u8.finish()) as ArrayRef,
            Arc::new(b_u16.finish()) as ArrayRef,
            Arc::new(b_u32.finish()) as ArrayRef,
            Arc::new(b_u64.finish()) as ArrayRef,
            Arc::new(b_f32.finish()) as ArrayRef,
            Arc::new(b_f64.finish()) as ArrayRef,
            Arc::new(b_b.finish()) as ArrayRef,
            Arc::new(b_str.finish()) as ArrayRef,
            Arc::new(b_ts.finish()) as ArrayRef,
            Arc::new(b_lstr.finish()) as ArrayRef,
            Arc::new(b_dec.finish()) as ArrayRef,
            Arc::new(b_group.finish()) as ArrayRef,
        ]
    };

    let rb = RecordBatch::try_new(Arc::new(schema), cols)?;

    Ok(vec![vec![rb]])
}
