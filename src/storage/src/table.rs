use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use arrow2::datatypes::Schema;
use lru::LruCache;
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

use crate::memtable::Memtable;
use crate::Fs;
use crate::KeyValue;
use crate::Stats;
use crate::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Options {
    pub parallelism: usize,
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
            parallelism: 1,
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
#[allow(dead_code)]
fn print_partitions(partitions: &[Partition]) {
    for (pid, p) in partitions.iter().enumerate() {
        println!("+-- {}", pid);
        for (lid, l) in p.levels.iter().enumerate() {
            println!("|  +-- {}", lid);
            for part in &l.parts {
                println!(
                    "|     +-- id {} min {:?},max {:?}, size {}",
                    part.id, part.min, part.max, part.size_bytes
                );
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Part {
    pub(crate) id: usize,
    pub(crate) size_bytes: u64,
    pub(crate) values: usize,
    pub(crate) min: Vec<KeyValue>,
    pub(crate) max: Vec<KeyValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Partition {
    pub(crate) id: usize,
    pub(crate) levels: Vec<Level>,
}

#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub(crate) name: String,
    pub(crate) memtable: Arc<Mutex<Memtable>>,
    pub(crate) metadata: Arc<Mutex<Metadata>>,
    pub(crate) vfs: Arc<Fs>,
    pub(crate) log: Arc<Mutex<BufWriter<File>>>,
    pub(crate) cas: Arc<RwLock<LruCache<Vec<Value>, i64>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Metadata {
    pub(crate) version: u64,
    pub(crate) seq_id: u64,
    pub(crate) log_id: u64,
    pub(crate) table_name: String,
    pub(crate) schema: Schema,
    pub(crate) levels: Vec<Level>,
    pub(crate) stats: Stats,
    pub(crate) opts: Options,
}

pub(crate) fn part_path(path: &Path, table_name: &str, level_id: usize, part_id: usize) -> PathBuf {
    path.join(format!(
        "tables/{}/{}/{}.parquet",
        table_name, level_id, part_id
    ))
}
