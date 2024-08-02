use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use arrow2::datatypes::{DataType, Schema, TimeUnit};
use lru::LruCache;
use parking_lot::Mutex;
use parking_lot::RwLock;
use prost::Message;
use serde::Deserialize;
use serde::Serialize;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use crate::memtable::Memtable;
use crate::{Fs, metadata};
use crate::KeyValue;
use crate::Stats;
use crate::Value;
use crate::error::Result;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub index_cols: usize,
    pub is_replacing: bool,
    pub levels: usize,
    pub l0_max_parts: usize,
    pub l1_max_size_bytes: usize,
    pub level_size_multiplier: usize,
    pub max_log_length_bytes: usize,
    pub merge_max_l1_part_size_bytes: usize,
    pub merge_part_size_multiplier: usize,
    pub merge_data_page_size_limit_bytes: Option<usize>,
    pub merge_row_group_values_limit: usize,
    pub merge_array_size: usize,
    pub merge_chunk_size: usize,
    pub merge_array_page_size: usize,
    pub merge_max_page_size: usize,
}

impl Options {
    pub fn test(is_replacing: bool) -> Self {
        Options {
            levels: 7,
            merge_array_size: 10000,
            index_cols: 1,
            is_replacing,
            l1_max_size_bytes: 1024 * 1024 * 10,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024 * 1024 * 100,
            merge_array_page_size: 10000,
            merge_data_page_size_limit_bytes: Some(1024 * 1024),
            merge_max_l1_part_size_bytes: 1024 * 1024,
            merge_part_size_multiplier: 10,
            merge_row_group_values_limit: 1000,
            merge_chunk_size: 1024 * 8 * 8,
            merge_max_page_size: 1024 * 1024,
        }
    }
}
pub fn print_partitions(levels: &[Level]) {
    for (lid, l) in levels.iter().enumerate() {
        println!("|  +-- {}", lid);
        for part in &l.parts {
            println!(
                "|     +-- id {} min {:?},max {:?}, size {}",
                part.id, part.min, part.max, part.size_bytes
            );
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Part {
    pub(crate) id: usize,
    pub(crate) size_bytes: u64,
    pub(crate) values: usize,
    pub(crate) min: Vec<KeyValue>,
    pub(crate) max: Vec<KeyValue>,
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) struct Level {
    pub(crate) part_id: usize,
    pub(crate) parts: Vec<Part>,
}

impl Level {
    pub(crate) fn new_empty() -> Self {
        Level {
            part_id: 0,
            parts: Vec::new(),
        }
    }

    pub(crate) fn get_part(&self, part: usize) -> Part {
        for p in &self.parts {
            if p.id == part {
                return p.clone();
            }
        }

        unreachable!("part not found")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Partition {
    pub(crate) id: usize,
    pub(crate) levels: Vec<Level>,
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) memtable: Arc<Mutex<Memtable>>,
    pub(crate) metadata: Arc<Mutex<Metadata>>,
    pub(crate) metadata_f: Arc<Mutex<BufWriter<File>>>,
    pub(crate) vfs: Arc<Fs>,
    pub(crate) log: Arc<Mutex<BufWriter<File>>>,
    pub(crate) cas: Arc<RwLock<LruCache<Vec<Value>, i64>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Metadata {
    pub(crate) version: u64,
    pub(crate) log_id: u64,
    pub(crate) table_name: String,
    pub(crate) schema: Schema,
    pub(crate) levels: Vec<Level>,
    pub(crate) stats: Stats,
    pub(crate) opts: Options,
}

pub(crate) fn part_path(path: &Path, table_name: &str, level_id: usize, part_id: usize) -> PathBuf {
    path.join(format!(
        "tables/{}/levels/{}/{}.parquet",
        table_name, level_id, part_id
    ))
}

pub fn serialize_md(md: &Metadata) -> Result<Vec<u8>> {
    // unimplemented!()
    let schema = metadata::Schema {
        fields: md.schema.fields.iter().map(|f| {
            metadata::Field {
                name: f.name.clone(),
                data_type: match f.data_type {
                    DataType::Utf8 => metadata::DataType::String as i32,
                    DataType::Int8 => metadata::DataType::Int8 as i32,
                    DataType::Int16 => metadata::DataType::Int16 as i32,
                    DataType::Int32 => metadata::DataType::Int32 as i32,
                    DataType::Int64 => metadata::DataType::Int64 as i32,
                    DataType::Decimal(_, _) => metadata::DataType::Decimal as i32,
                    DataType::Boolean => metadata::DataType::Boolean as i32,
                    DataType::Timestamp(_, _) => metadata::DataType::Timestamp as i32,
                    _ => unimplemented!("{:?}", f.data_type)
                },
                is_nullable: f.is_nullable,
            }
        }).collect::<Vec<_>>()
    };

    let levels = md.levels.iter().map(|l| {
        metadata::Level {
            part_id: l.part_id as u64,
            parts: l.parts.iter().map(|p| {
                let min_max = |kv: &KeyValue| -> metadata::KeyValue{
                    let mut int8 = None;
                    let mut int16 = None;
                    let mut int32 = None;
                    let mut int64 = None;
                    let mut string = None;
                    let mut timestamp = None;
                    match kv {
                        KeyValue::Int8(v) => int8 = Some(*v as u32),
                        KeyValue::Int16(v) => int16 = Some(*v as u32),
                        KeyValue::Int32(v) => int32 = Some(*v as u32),
                        KeyValue::Int64(v) => int64 = Some(*v as u64),
                        KeyValue::String(v) => string = Some(v.clone()),
                        KeyValue::Timestamp(v) => timestamp = Some(*v as u64),
                    }
                    metadata::KeyValue {
                        int8,
                        int16,
                        int32,
                        int64,
                        string,
                        timestamp,
                    }
                };

                metadata::Part {
                    id: p.id as u64,
                    size_bytes: p.size_bytes,
                    values: p.values as u64,
                    min: p.min.iter().map(|m| {
                        min_max(m)
                    }).collect::<Vec<_>>(),
                    max: p.max.iter().map(|m| {
                        min_max(m)
                    }).collect::<Vec<_>>(),
                }
            }).collect::<Vec<_>>(),
        }
    }).collect::<Vec<_>>();
    let md = crate::metadata::Metadata {
        version: md.version as u32,
        log_id: md.log_id,
        table_name: md.table_name.clone(),
        schema: Some(schema),
        levels,
        stats: Some(metadata::Stats {
            resident_bytes: md.stats.resident_bytes,
            on_disk_bytes: md.stats.on_disk_bytes,
            logged_bytes: md.stats.logged_bytes,
            written_bytes: md.stats.written_bytes,
            read_bytes: md.stats.read_bytes,
            space_amp: md.stats.space_amp,
            write_amp: md.stats.write_amp,
        }),
        options: Some(metadata::Options {
            index_cols: md.opts.index_cols as u64,
            is_replacing: md.opts.is_replacing,
            levels: md.opts.levels as u64,
            l0_max_parts: md.opts.l0_max_parts as u64,
            l1_max_size_bytes: md.opts.l1_max_size_bytes as u64,
            level_size_multiplier: md.opts.level_size_multiplier as u64,
            max_log_length_bytes: md.opts.max_log_length_bytes as u64,
            merge_max_l1_part_size_bytes: md.opts.merge_max_l1_part_size_bytes as u64,
            merge_part_size_multiplier: md.opts.merge_part_size_multiplier as u64,
            merge_data_page_size_limit_bytes: md.opts.merge_data_page_size_limit_bytes.map(|v| v as u64),
            merge_row_group_values_limit: md.opts.merge_row_group_values_limit as u64,
            merge_array_size: md.opts.merge_array_size as u64,
            merge_chunk_size: md.opts.merge_chunk_size as u64,
            merge_array_page_size: md.opts.merge_array_page_size as u64,
            merge_max_page_size: md.opts.merge_max_page_size as u64,
        }),
    };

    Ok(md.encode_to_vec())
}

pub fn deserialize_md(data: &[u8]) -> Result<Metadata> {
    let from = metadata::Metadata::decode(data.as_ref())?;

    let md = Metadata {
        version: from.version as u64,
        log_id: from.log_id,
        table_name: from.table_name.clone(),
        schema: Schema {
            fields: from.schema.unwrap().fields.iter().map(|f| {
                let data_type = match f.data_type {
                    1 => DataType::Utf8,
                    2 => DataType::Int8,
                    3 => DataType::Int16,
                    4 => DataType::Int32,
                    5 => DataType::Int64,
                    6 => DataType::Decimal(DECIMAL_PRECISION as usize, DECIMAL_SCALE as usize),
                    7 => DataType::Boolean,
                    8 => DataType::Timestamp(TimeUnit::Millisecond, None),
                    _ => unimplemented!("{:?}", f.data_type)
                };
                arrow2::datatypes::Field::new(&f.name, data_type, f.is_nullable)
            }).collect::<Vec<_>>(),
            metadata: Default::default(),
        },
        levels: from.levels.iter().map(|l| {
            Level {
                part_id: l.part_id as usize,
                parts: l.parts.iter().map(|p| {
                    let min_max = |kv: &metadata::KeyValue| -> KeyValue {
                        if let Some(v) = kv.int8 {
                            KeyValue::Int8(v as i8)
                        } else if let Some(v) = kv.int16 {
                            KeyValue::Int16(v as i16)
                        } else if let Some(v) = kv.int32 {
                            KeyValue::Int32(v as i32)
                        } else if let Some(v) = kv.int64 {
                            KeyValue::Int64(v as i64)
                        } else if let Some(v) = kv.string.clone() {
                            KeyValue::String(v)
                        } else if let Some(v) = kv.timestamp {
                            KeyValue::Timestamp(v as i64)
                        } else {
                            unreachable!()
                        }
                    };

                    Part {
                        id: p.id as usize,
                        size_bytes: p.size_bytes,
                        values: p.values as usize,
                        min: p.min.iter().map(|m| {
                            min_max(m)
                        }).collect::<Vec<_>>(),
                        max: p.max.iter().map(|m| {
                            min_max(m)
                        }).collect::<Vec<_>>(),
                    }
                }).collect::<Vec<_>>(),
            }
        }).collect::<Vec<_>>(),
        stats: Stats {
            resident_bytes: from.stats.unwrap().resident_bytes,
            on_disk_bytes: from.stats.unwrap().on_disk_bytes,
            logged_bytes: from.stats.unwrap().logged_bytes,
            written_bytes: from.stats.unwrap().written_bytes,
            read_bytes: from.stats.unwrap().read_bytes,
            space_amp: from.stats.unwrap().space_amp,
            write_amp: from.stats.unwrap().write_amp,
        },
        opts: Options {
            index_cols: from.options.unwrap().index_cols as usize,
            is_replacing: from.options.unwrap().is_replacing,
            levels: from.options.unwrap().levels as usize,
            l0_max_parts: from.options.unwrap().l0_max_parts as usize,
            l1_max_size_bytes: from.options.unwrap().l1_max_size_bytes as usize,
            level_size_multiplier: from.options.unwrap().level_size_multiplier as usize,
            max_log_length_bytes: from.options.unwrap().max_log_length_bytes as usize,
            merge_max_l1_part_size_bytes: from.options.unwrap().merge_max_l1_part_size_bytes as usize,
            merge_part_size_multiplier: from.options.unwrap().merge_part_size_multiplier as usize,
            merge_data_page_size_limit_bytes: from.options.unwrap().merge_data_page_size_limit_bytes.map(|v| v as usize),
            merge_row_group_values_limit: from.options.unwrap().merge_row_group_values_limit as usize,
            merge_array_size: from.options.unwrap().merge_array_size as usize,
            merge_chunk_size: from.options.unwrap().merge_chunk_size as usize,
            merge_array_page_size: from.options.unwrap().merge_array_page_size as usize,
            merge_max_page_size: from.options.unwrap().merge_max_page_size as usize,
        },
    };

    Ok(md)
}

#[cfg(test)]
mod tests {
    use arrow2::datatypes::{DataType, TimeUnit};
    use chrono::{DateTime, Utc};
    use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
    use crate::KeyValue;
    use crate::table::{deserialize_md, Level, Metadata, Options, Part, serialize_md};

    #[test]
    fn test_roundtrip() {
        let md = Metadata {
            version: 1,
            log_id: 1,
            table_name: "test".to_string(),
            schema: arrow2::datatypes::Schema {
                fields: vec![
                    arrow2::datatypes::Field::new("a", DataType::Int8, true),
                    arrow2::datatypes::Field::new("b", DataType::Int16, true),
                    arrow2::datatypes::Field::new("c", DataType::Int32, true),
                    arrow2::datatypes::Field::new("d", DataType::Int64, true),
                    arrow2::datatypes::Field::new("e", DataType::Utf8, true),
                    arrow2::datatypes::Field::new("f", DataType::Decimal(DECIMAL_PRECISION as usize, DECIMAL_SCALE as usize), true),
                    arrow2::datatypes::Field::new("g", DataType::Boolean, true),
                    arrow2::datatypes::Field::new("h", DataType::Timestamp(TimeUnit::Millisecond, None), true),
                ],
                metadata: Default::default(),
            },
            levels: vec![
                Level {
                    part_id: 0,
                    parts: vec![
                        Part {
                            id: 0,
                            size_bytes: 100,
                            values: 100,
                            min: vec![
                                KeyValue::Int32(1),
                                KeyValue::String("a".to_string()),
                                KeyValue::Int64(1),
                                KeyValue::Timestamp(1),
                            ],
                            max: vec![
                                KeyValue::Int32(1),
                                KeyValue::String("a".to_string()),
                                KeyValue::Int64(1),
                                KeyValue::Timestamp(1),
                            ],
                        }
                    ],
                }
            ],
            stats: Default::default(),
            opts: Options {
                index_cols: 1,
                is_replacing: true,
                levels: 2,
                l0_max_parts: 3,
                l1_max_size_bytes: 4,
                level_size_multiplier: 5,
                max_log_length_bytes: 6,
                merge_max_l1_part_size_bytes: 7,
                merge_part_size_multiplier: 8,
                merge_data_page_size_limit_bytes: Some(9),
                merge_row_group_values_limit: 10,
                merge_array_size: 11,
                merge_chunk_size: 12,
                merge_array_page_size: 13,
                merge_max_page_size: 14,
            },
        };

        let data = serialize_md(&md).unwrap();
        let md2 = deserialize_md(&data).unwrap();

        assert_eq!(md, md2);
    }
}