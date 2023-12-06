mod compaction;

use std::cmp;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::hash::Hasher;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::mem;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{mpsc, OnceLock};
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time;
use std::time::Duration;
use std::time::Instant;

use arrow::compute::concat;
use arrow::ipc::Time;
use arrow2::array::Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::MutableArray;
use arrow2::array::MutableBinaryArray;
use arrow2::array::MutableBooleanArray;
use arrow2::array::MutablePrimitiveArray;
use arrow2::array::MutableUtf8Array;
use arrow2::array::PrimitiveArray;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, TimeUnit};
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::datatypes::SchemaRef;
use arrow2::io::parquet;
use arrow2::io::parquet::read;
use arrow2::io::parquet::read::FileReader;
use arrow2::io::parquet::write::transverse;
use arrow_buffer::ToByteSlice;
use bincode::deserialize;
use bincode::serialize;
use chrono::format::Item;
use crossbeam_skiplist::SkipSet;
use futures::select;
use futures::Stream;
use lazy_static::lazy_static;
use metrics::counter;
use metrics::describe_counter;
use metrics::describe_histogram;
use metrics::histogram;
use metrics::Counter;
use metrics::Gauge;
use metrics::Histogram;
use metrics::Unit;
use parking_lot::Mutex;
use parking_lot::RwLock;
use parquet2::metadata::FileMetaData;
use rand::rngs::mock::StepRng;
use rand::Rng;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use shuffle::irs::Irs;
use shuffle::shuffler::Shuffler;
use siphasher::sip::SipHasher13;
use tracing::debug;
use tracing::error;
use tracing::instrument;
use tracing::trace;
use tracing_subscriber::fmt::time::FormatTime;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use common::types::DType;
use crate::arrow_conversion::schema2_to_schema1;

use crate::db::compaction::Compactor;
use crate::db::compaction::CompactorMessage;
use crate::error::Result;
use crate::error::StoreError;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::parquet::merger;
use crate::parquet::merger::arrow_merger;
use crate::parquet::merger::arrow_merger::MemChunkIterator;
use crate::parquet::merger::arrow_merger::MergingIterator;
use crate::parquet::merger::chunk_min_max;
use crate::KeyValue;
use crate::NamedValue;
use crate::Value;

macro_rules! memory_col_to_arrow {
    ($col:expr, $dt:ident,$arr_ty:ident) => {{
        let vals = $col
            .into_iter()
            .map(|v| match v {
                Value::$dt(b) => b,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        $arr_ty::from(vals).as_box()
    }};
}


pub enum DBDataType {
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Boolean,
    Timestamp,
    Decimal,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Part {
    id: usize,
    size_bytes: u64,
    min: Vec<KeyValue>,
    max: Vec<KeyValue>,
}

impl Part {
    fn path(path: &PathBuf, partition_id: usize, level: usize, id: usize) -> PathBuf {
        PathBuf::from(path.join(format!("parts/{}/{}/{}.parquet", partition_id, level, id)))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Level {
    part_id: usize,
    parts: Vec<Part>,
}

impl Level {
    fn new_empty() -> Self {
        Level {
            part_id: 0,
            parts: Vec::new(),
        }
    }

    fn get_part(&self, part: usize) -> Part {
        for p in &self.parts {
            if p.id == part {
                return p.clone();
            }
        }

        unreachable!("part not found")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Partition {
    id: usize,
    levels: Vec<Level>,
}

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    memtable: Arc<Mutex<SkipSet<MemOp>>>,
    metadata: Arc<Mutex<Metadata>>,
    vfs: Arc<Vfs>,
    log: Arc<Mutex<BufWriter<File>>>,
}

struct TableStats {
    insert_time_ms: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metadata {
    version: u64,
    seq_id: u64,
    log_id: u64,
    table_name: String,
    schema: Schema,
    partitions: Vec<Partition>,
    stats: Stats,
    opts: TableOptions,
}

#[derive(Debug, Serialize, Deserialize, Hash)]
pub enum Op {
    Insert(Vec<KeyValue>, Vec<Value>),
    Delete(Vec<KeyValue>),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LogOp {
    Insert(Vec<KeyValue>, Vec<Value>),
    Metadata(Metadata),
}

#[derive(Debug)]
struct MemOp {
    op: Op,
    seq_id: u64,
}

impl PartialOrd for MemOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MemOp {
    fn eq(&self, other: &Self) -> bool {
        let left_k = match &self.op {
            Op::Insert(k, _) => k.to_owned(),
            Op::Delete(k) => k.to_owned(),
        };

        let right_k = match &other.op {
            Op::Insert(k, _) => k.to_owned(),
            Op::Delete(k) => k.to_owned(),
        };

        if left_k == right_k {
            self.seq_id == other.seq_id
        } else {
            false
        }
    }
}

impl Ord for MemOp {
    fn cmp(&self, other: &Self) -> Ordering {
        let left_k = match &self.op {
            Op::Insert(k, _) => k.to_owned(),
            Op::Delete(k) => k.to_owned(),
        };

        let right_k = match &other.op {
            Op::Insert(k, _) => k.to_owned(),
            Op::Delete(k) => k.to_owned(),
        };

        match left_k.partial_cmp(&right_k) {
            None => unreachable!("keys must be comparable"),
            Some(ord) => match ord {
                Ordering::Equal => self.seq_id.cmp(&other.seq_id),
                _ => ord,
            },
        }
    }
}

impl Eq for MemOp {}

fn hash_crc32(v: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(v);

    // we XOR the hash to make sure it's something other than 0 when empty,
    // because 0 is an easy value to create accidentally or via corruption.
    hasher.finalize() ^ 0xFF
}

fn siphash<T: Hash>(t: &T) -> u64 {
    let mut h = SipHasher13::new();
    t.hash(&mut h);
    h.finish()
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Stats {
    pub resident_bytes: u64,
    pub on_disk_bytes: u64,
    pub logged_bytes: u64,
    pub written_bytes: u64,
    pub read_bytes: u64,
    pub space_amp: f64,
    pub write_amp: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableOptions {
    pub partitions: usize,
    pub index_cols: usize,
    pub levels: usize,
    pub l0_max_parts: usize,
    pub l1_max_size_bytes: usize,
    pub level_size_multiplier: usize,
    pub max_log_length_bytes: usize,
    pub merge_max_l1_part_size_bytes: usize,
    pub merge_part_size_multiplier: usize,
    pub merge_index_cols: usize,
    pub merge_data_page_size_limit_bytes: Option<usize>,
    pub merge_row_group_values_limit: usize,
    pub merge_array_size: usize,
    pub merge_array_page_size: usize,
}

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

fn part_path(
    path: &PathBuf,
    table_name: &str,
    partition_id: usize,
    level_id: usize,
    part_id: usize,
) -> PathBuf {
    path.join(format!(
        "tables/{}/{}/{}/{}.parquet",
        table_name, partition_id, level_id, part_id
    ))
}

fn level_path(path: &PathBuf, table_name: &str, partition_id: usize, level_id: usize) -> PathBuf {
    path.join(format!(
        "tables/{}/{}/{}",
        table_name, partition_id, level_id
    ))
}

#[derive(Debug)]
pub struct Vfs {
    // not RWLock because of possible deadlocks
    lock: Mutex<()>,
}

impl Vfs {
    pub fn new() -> Self {
        Self {
            lock: Default::default(),
        }
    }

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let _g = self.lock.lock();
        Ok(fs::remove_file(path)?)
    }

    pub fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> Result<()> {
        let _g = self.lock.lock();
        Ok(fs::rename(from, to)?)
    }
}

enum FsOp {
    Rename(PathBuf, PathBuf),
    Delete(PathBuf),
}

#[derive(Clone, Debug)]
pub struct Options {}

#[derive(Clone, Debug)]
pub struct OptiDBImpl {
    pub opts: Options,
    tables: Arc<RwLock<Vec<Table>>>,
    compactor_outbox: Sender<CompactorMessage>,
    path: PathBuf,
}

fn _log(op: LogOp, metadata: &mut Metadata, log: &mut File) -> Result<usize> {
    let data = serialize(&op)?;

    let crc = hash_crc32(&data);
    // !@#trace!("crc32: {}", crc);
    log.write_all(&crc.to_le_bytes())?;
    log.write_all(&(data.len() as u64).to_le_bytes())?;
    log.write_all(&data)?;
    let logged_size = 8 + 4 + data.len();
    // !@#trace!("logged size: {}", logged_size);

    Ok(logged_size)
}

fn recover_op(op: LogOp, memtable: &mut SkipSet<MemOp>, metadata: &mut Metadata) -> Result<()> {
    match op {
        LogOp::Insert(k, v) => {
            memtable.insert(MemOp {
                op: Op::Insert(k, v),
                seq_id: metadata.seq_id,
            });
        }
        LogOp::Metadata(md) => *metadata = md,
    }

    metadata.seq_id += 1;

    Ok(())
}

fn log_metadata(log: &mut File, metadata: &mut Metadata) -> Result<()> {
    // !@#trace!("log metadata");
    _log(LogOp::Metadata(metadata.clone()), metadata, log)?;
    metadata.seq_id += 1;

    Ok(())
}

fn try_recover_table(path: PathBuf, name: String, opts: Option<TableOptions>) -> Result<Table> {
    let start_time = Instant::now();
    // !@#trace!("starting recovery");
    let dir = fs::read_dir(path.clone())?;
    let mut logs = BinaryHeap::new();

    for f in dir {
        let f = f?;
        match f.path().extension() {
            None => {}
            Some(n) => {
                if n == OsStr::new("log") {
                    logs.push(f.path());
                }
            }
        };
    }

    // !@#trace!("found {} logs", logs.len());

    let opts = opts.unwrap_or(TableOptions {
        partitions: 0,
        index_cols: 0,
        levels: 0,
        l0_max_parts: 0,
        l1_max_size_bytes: 0,
        level_size_multiplier: 0,
        max_log_length_bytes: 0,
        merge_max_l1_part_size_bytes: 0,
        merge_part_size_multiplier: 0,
        merge_index_cols: 0,
        merge_data_page_size_limit_bytes: None,
        merge_row_group_values_limit: 0,
        merge_array_size: 0,
        merge_array_page_size: 0,
    });

    for pid in 0..opts.partitions {
        for lid in 0..opts.levels {
            fs::create_dir_all(path.join(pid.to_string()).join(lid.to_string()))?;
        }
    }

    let mut metadata = Metadata {
        version: 0,
        seq_id: 0,
        log_id: 0,
        table_name: name.clone(),
        schema: Schema::from(vec![]),
        stats: Default::default(),
        partitions: (0..opts.partitions)
            .into_iter()
            .map(|pid| Partition {
                id: pid,
                levels: vec![Level::new_empty(); opts.levels],
            })
            .collect::<Vec<_>>(),
        opts,
    };

    let mut memtable = SkipSet::new();
    let log = if let Some(log_path) = logs.pop() {
        // !@#trace!("last log: {:?}", log_path);
        // todo make a buffered read. Currently something odd happens with reading of the length of the record
        let mut f = File::open(&log_path)?;

        let mut ops = 0;
        let mut read_bytes = 0;

        loop {
            let mut crc_b = [0u8; mem::size_of::<u32>()];
            read_bytes = f.read(&mut crc_b)?;
            if read_bytes == 0 {
                break;
            }
            let crc32 = u32::from_le_bytes(crc_b);
            // !@#trace!("crc32: {}", crc32);

            let mut len_b = [0u8; mem::size_of::<u64>()];
            f.read(&mut len_b)?;
            let len = u64::from_le_bytes(len_b);
            // !@#trace!("len: {}", len);
            let mut data_b = vec![0u8; len as usize];
            f.read(&mut data_b)?;
            // todo recover from this case
            let cur_crc32 = hash_crc32(&data_b);
            if crc32 != cur_crc32 {
                return Err(StoreError::Internal(format!(
                    "corrupted log. crc32 is: {}, need: {}",
                    cur_crc32, crc32
                )));
            }
            let op = deserialize::<LogOp>(&data_b)?;
            recover_op(op, &mut memtable, &mut metadata)?;

            metadata.seq_id += 1;
            ops += 1;
        }
        // !@#trace!("operations recovered: {}", ops);
        drop(f);

        OpenOptions::new().append(true).open(log_path)?
    } else {
        // !@#trace!("creating initial log {}", format!("{:016}.log", 0));

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.join(format!("{:016}.log", 0)))?
    };

    metadata.stats.logged_bytes = log.metadata().unwrap().len();

    let log = BufWriter::new(log);
    // if trigger_compact {
    //     // compactor_outbox.send(CompactorMessage::Compact).unwrap();
    // }
    histogram!("store.insert_time_sec",start_time.elapsed());
    Ok(Table {
        name,
        memtable: Arc::new(Mutex::new(memtable)),
        metadata: Arc::new(Mutex::new(metadata)),
        vfs: Arc::new(Vfs::new()),
        log: Arc::new(Mutex::new(log)),
    })
}

// #[instrument(level = "trace")]
fn recover(path: PathBuf, opts: Options) -> Result<OptiDBImpl> {
    // Create a background thread which checks for deadlocks every 1s

    fs::create_dir_all(path.join("tables"))?;
    let mut tables = vec![];
    let dir = fs::read_dir(path.join("tables"))?;
    for f in dir {
        let f = f?;
        tables.push(try_recover_table(
            f.path(),
            f.file_name().to_os_string().into_string().unwrap(),
            None,
        )?);
    }

    // let trigger_compact = if metadata.partitions[0].parts.len() > 0
    // && metadata.partitions[0].parts.len() > opts.l0_max_parts
    // {
    // true
    // } else {
    // false
    // };
    let (compactor_outbox, rx) = std::sync::mpsc::channel();
    let tables = Arc::new(RwLock::new(tables));
    let compactor = Compactor::new(tables.clone(), path.clone(), rx);
    thread::spawn(move || compactor.run());
    // if trigger_compact {
    //     // compactor_outbox.send(CompactorMessage::Compact).unwrap();
    // }
    Ok(OptiDBImpl {
        opts,
        tables,
        compactor_outbox,
        path,
    })
}

#[derive(Debug)]
struct MemtablePartitionChunk {
    partition_id: usize,
    chunk: Chunk<Box<dyn Array>>,
}

#[derive(Debug)]
struct MemtablePartition {
    id: usize,
    cols: Vec<Vec<Value>>,
}

impl MemtablePartition {
    pub fn new(id: usize, fields: usize) -> Self {
        Self {
            id,
            cols: vec![Vec::new(); fields],
        }
    }
}

fn memtable_to_partitioned_chunks(
    metadata: &Metadata,
    memtable: &SkipSet<MemOp>,
    partition_id: Option<usize>,
) -> Result<Vec<MemtablePartitionChunk>> {
    let mut partitions: Vec<MemtablePartition> = vec![];

    for op in memtable.iter() {
        let (keys, vals) = match &op.op {
            Op::Insert(k, v) => (k, Some(v)),
            Op::Delete(k) => (k, None),
        };
        let phash = siphash(
            &keys[0..metadata.opts.index_cols]
                .into_iter()
                .collect::<Vec<_>>(),
        );
        if let Some(pid) = partition_id {
            if pid != phash as usize % metadata.opts.partitions {
                continue;
            }
        }

        let mut pid: isize = -1;
        for (pidx, p) in partitions.iter().enumerate() {
            if p.id == phash as usize % metadata.opts.partitions {
                pid = pidx as isize;
                break;
            }
            continue;
        }
        if pid < 0 {
            partitions.push(MemtablePartition::new(
                phash as usize % metadata.opts.partitions,
                metadata.schema.fields.len(),
            ));
            pid = partitions.len() as isize - 1;
        }
        let pid = pid as usize;
        let idx_offset = keys.len();
        for (idx, key) in keys.into_iter().enumerate() {
            partitions[pid].cols[idx].push(key.into());
        }
        if let Some(v) = vals {
            for (idx, val) in v.into_iter().enumerate() {
                partitions[pid].cols[idx_offset + idx].push(val.clone());
            }

            for i in 0..metadata.schema.fields.len() - (metadata.opts.index_cols + v.len()) {
                partitions[pid].cols[idx_offset + v.len() + i].push(Value::null(
                    &metadata.schema.fields[idx_offset + v.len() + i].data_type,
                ));
            }
        }
    }
    // todo make nulls for missing values (schema evolution)
    let v = partitions
        .into_iter()
        .map(|p| {
            let arrs = p
                .cols
                .into_iter()
                .enumerate()
                .map(|(idx, col)| {
                    match metadata.schema.fields[idx].data_type {
                        DataType::Boolean => {
                            memory_col_to_arrow!(col, Boolean, MutableBooleanArray)
                        }
                        DataType::Int8 => memory_col_to_arrow!(col, Int8, MutablePrimitiveArray),
                        DataType::Int16 => memory_col_to_arrow!(col, Int16, MutablePrimitiveArray),
                        DataType::Int32 => memory_col_to_arrow!(col, Int32, MutablePrimitiveArray),
                        DataType::Int64 => memory_col_to_arrow!(col, Int64, MutablePrimitiveArray),
                        DataType::Timestamp(_, _) => {
                            let vals = col
                                .into_iter()
                                .map(|v| match v {
                                    Value::Int64(b) => b,
                                    _ => unreachable!("{:?}", v),
                                })
                                .collect::<Vec<_>>();
                            MutablePrimitiveArray::<i64>::from(vals).to(DataType::Timestamp(TimeUnit::Nanosecond, None)).as_box()
                        }
                        DataType::Utf8 => {
                            let vals = col
                                .into_iter()
                                .map(|v| match v {
                                    Value::String(b) => b,
                                    _ => unreachable!("{:?}", v),
                                })
                                .collect::<Vec<_>>();
                            MutableUtf8Array::<i32>::from(vals).as_box()
                        }
                        DataType::Decimal(_, _) => {
                            let vals = col
                                .into_iter()
                                .map(|v| match v {
                                    Value::Decimal(b) => b,
                                    _ => unreachable!("{:?}", v),
                                })
                                .collect::<Vec<_>>();
                            MutablePrimitiveArray::<i128>::from(vals).to(DataType::Decimal(DECIMAL_PRECISION as usize, DECIMAL_SCALE as usize)).as_box()
                        }
                        DataType::List(_) => unimplemented!(),
                        _ => unimplemented!(),
                    }
                })
                .collect::<Vec<_>>();
            (p.id, Chunk::new(arrs))
        })
        .collect::<Vec<_>>();

    let chunks = v
        .into_iter()
        .map(|(pid, chunk)| MemtablePartitionChunk {
            partition_id: pid,
            chunk,
        })
        .collect::<Vec<_>>();

    Ok(chunks)
}

fn write_level0(
    metadata: &Metadata,
    memtable: &SkipSet<MemOp>,
    path: PathBuf,
) -> Result<Vec<(usize, Part)>> {
    let mut ret = vec![];
    let partitioned_chunks = memtable_to_partitioned_chunks(metadata, memtable, None)?;
    for chunk in partitioned_chunks.into_iter() {
        let (min, max) = chunk_min_max(&chunk.chunk, metadata.opts.index_cols);

        let popts = parquet::write::WriteOptions {
            write_statistics: true,
            compression: parquet::write::CompressionOptions::Snappy,
            version: parquet::write::Version::V1,
            // todo define page size
            data_pagesize_limit: None,
        };

        let encodings = metadata
            .schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| parquet::write::Encoding::Plain))
            .collect();
        let row_groups = parquet::write::RowGroupIterator::try_new(
            vec![Ok(chunk.chunk)].into_iter(),
            &metadata.schema,
            popts,
            encodings,
        )?;

        let path = part_path(
            &path,
            metadata.table_name.as_str(),
            chunk.partition_id,
            0,
            metadata.partitions[chunk.partition_id].levels[0].part_id,
        );
        // !@#trace!("creating part file {:?}", p);
        let w = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut writer = parquet::write::FileWriter::try_new(BufWriter::new(w), metadata.schema.clone(), popts)?;
        // Write the part
        for group in row_groups {
            writer.write(group?)?;
        }
        let sz = writer.end(None)?;
        ret.push((chunk.partition_id, Part {
            id: metadata.partitions[chunk.partition_id].levels[0].part_id,
            size_bytes: sz,
            min,
            max,
        }))
    }
    // !@#trace!("{:?} bytes written", sz);
    Ok(ret)
}

fn flush(
    log: &mut File,
    memtable: &mut SkipSet<MemOp>,
    metadata: &mut Metadata,
    path: &PathBuf,
) -> Result<()> {
    let start_time = Instant::now();
    // in case when it is flushed by another thread
    if memtable.len() == 0 {
        return Ok(());
    }
    // !@#trace!("flushing log file");
    log.flush()?;
    log.sync_all()?;
    // swap memtable
    // !@#trace!("swapping memtable");

    // write to parquet
    // !@#trace!("writing to parquet");
    // !@#trace!("part id: {}", md.levels[0].part_id);
    let memtable = mem::take(memtable);
    let parts = write_level0(&metadata, &memtable, path.to_owned())?;

    for (pid, part) in parts {
        metadata.partitions[pid].levels[0].parts.push(part.clone());
        // increment table id
        metadata.partitions[pid].levels[0].part_id += 1;
        metadata.stats.written_bytes += part.size_bytes;
    }

    metadata.stats.logged_bytes = 0;
    // increment log id
    metadata.log_id += 1;

    // create new log
    trace!(
        "creating new log file {:?}",
        format!("{:016}.log", metadata.log_id)
    );

    let mut log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path.join(format!(
            "tables/{}/{:016}.log",
            metadata.table_name, metadata.log_id
        )))?;

    // write metadata to log as first record
    log_metadata(&mut log, metadata)?;

    trace!(
        "removing previous log file {:?}",
        format!("{:016}.log", metadata.log_id - 1)
    );
    fs::remove_file(path.join(format!(
        "tables/{}/{:016}.log",
        metadata.table_name,
        metadata.log_id - 1
    )))?;

    histogram!("store.flush_time_sec", start_time.elapsed());
    Ok(())
}

pub struct ScanStream {
    iter: Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>,
    start_time: Instant,
    table_name: String, // for metrics
}

impl ScanStream {
    pub fn new(
        iter: Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>,
        table_name: String,
    ) -> Self {
        Self {
            iter,
            start_time: Instant::now(),
            table_name,
        }
    }
}

impl Stream for ScanStream {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.next() {
            None => {
                histogram!("store.scan_time_sec",self.start_time.elapsed(),"table"=>self.table_name.to_string());
                Poll::Ready(None)
            }
            Some(chunk) => {
                Poll::Ready(Some(chunk))
            }
        }
    }
}

struct PrintHandle(metrics::Key);

impl metrics::CounterFn for PrintHandle {
    fn increment(&self, value: u64) {
        println!("counter increment for '{}': {}", self.0, value);
    }

    fn absolute(&self, value: u64) {
        println!("counter absolute for '{}': {}", self.0, value);
    }
}

impl metrics::GaugeFn for PrintHandle {
    fn increment(&self, value: f64) {
        println!("gauge increment for '{}': {}", self.0, value);
    }

    fn decrement(&self, value: f64) {
        println!("gauge decrement for '{}': {}", self.0, value);
    }

    fn set(&self, value: f64) {
        println!("gauge set for '{}': {}", self.0, value);
    }
}

impl metrics::HistogramFn for PrintHandle {
    fn record(&self, value: f64) {
        println!("histogram record for '{}': {}", self.0, value);
    }
}

#[derive(Default)]
struct PrintRecorder;

impl metrics::Recorder for PrintRecorder {
    fn describe_counter(
        &self,
        key_name: metrics::KeyName,
        unit: Option<Unit>,
        description: metrics::SharedString,
    ) {
        println!(
            "(counter) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_gauge(
        &self,
        key_name: metrics::KeyName,
        unit: Option<Unit>,
        description: metrics::SharedString,
    ) {
        println!(
            "(gauge) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn describe_histogram(
        &self,
        key_name: metrics::KeyName,
        unit: Option<Unit>,
        description: metrics::SharedString,
    ) {
        println!(
            "(histogram) registered key {} with unit {:?} and description {:?}",
            key_name.as_str(),
            unit,
            description
        );
    }

    fn register_counter(&self, key: &metrics::Key) -> Counter {
        Counter::from_arc(Arc::new(PrintHandle(key.clone())))
    }

    fn register_gauge(&self, key: &metrics::Key) -> Gauge {
        Gauge::from_arc(Arc::new(PrintHandle(key.clone())))
    }

    fn register_histogram(&self, key: &metrics::Key) -> Histogram {
        Histogram::from_arc(Arc::new(PrintHandle(key.clone())))
    }
}

fn init_print_logger() {
    let recorder = PrintRecorder::default();
    metrics::set_boxed_recorder(Box::new(recorder)).unwrap()
}


impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        init_print_logger();
        describe_counter!("store.inserts_count", "number of inserts processed");
        describe_histogram!("store.insert_time_sec", Unit::Seconds, "insert time");
        describe_counter!("store.scans_count", "number of scans processed");
        describe_histogram!("store.scan_time_sec", Unit::Seconds, "scan time");
        describe_histogram!(
            "store.scan_memtable_time_sec",
            Unit::Seconds,
            "scan memtable time"
        );
        describe_counter!("store.compactions_count", "number of compactions");
        describe_histogram!("store.compaction_time_sec", Unit::Microseconds, "compaction time");
        describe_histogram!("store.recovery_time_sec", Unit::Microseconds, "recovery time");

        recover(path, opts)
    }

    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&self, tbl_name: &str, mut values: Vec<NamedValue>) -> Result<()> {
        counter!("store.inserts_count", 1,"table"=>tbl_name.to_string());
        let start_time = Instant::now();
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();

        drop(tables);

        let mut metadata = tbl.metadata.lock();

        let pk_values = values
            .drain(0..metadata.opts.index_cols)
            .map(|v| KeyValue::from(&v.value))
            .collect::<Vec<_>>();
        let mut final_values: Vec<Value> = vec![];

        for (field_idx, field) in metadata
            .schema
            .fields
            .iter()
            .skip(metadata.opts.index_cols)
            .enumerate()
        {
            let mut found = false;
            for v in values.iter() {
                if field.name == v.name {
                    final_values.push(v.value.clone());
                    found = true;
                    break;
                }
            }

            if found {
                continue;
            }

            final_values.push(Value::null(field.data_type()));
        }
        let mut log = tbl.log.lock();
        let logged = _log(
            LogOp::Insert(pk_values.clone(), final_values.clone()),
            &mut metadata,
            log.get_mut(),
        )?;
        metadata.stats.logged_bytes += logged as u64;
        metadata.seq_id += 1;

        let mut memtable = tbl.memtable.lock();

        memtable.insert(MemOp {
            op: Op::Insert(pk_values, final_values),
            seq_id: metadata.seq_id,
        });

        if metadata.stats.logged_bytes as usize > metadata.opts.max_log_length_bytes {
            flush(log.get_mut(), &mut memtable, &mut metadata, &self.path)?;

            // if md.levels[0].parts.len() > 0 && md.levels[0].parts.len() > self.opts.l0_max_parts {
            //     self.compactor_outbox
            //         .send(CompactorMessage::Compact)
            //         .unwrap();
            // }
        }
        histogram!("store.insert_time_sec",start_time.elapsed(),"table"=>tbl_name.to_string());
        // !@#debug!("insert finished in {: ?}", duration);
        Ok(())
    }

    pub fn compact(&self) {
        self.compactor_outbox
            .send(CompactorMessage::Compact)
            .unwrap();
    }

    pub fn flush(&self) -> Result<()> {
        let mut tbls = self.tables.read();
        for tbl in tbls.iter() {
            let mut log = tbl.log.lock();
            let mut memtable = tbl.memtable.lock();
            let mut metadata = tbl.metadata.lock();
            flush(log.get_mut(), &mut memtable, &mut metadata, &self.path)?;
        }

        Ok(())
    }

    pub fn schema(&self, tbl_name: &str) -> Result<Schema> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let mut tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl,
        };

        let schema = tbl.metadata.lock().schema.clone();
        Ok(schema)
    }

    pub fn schema1(&self, tbl_name: &str) -> Result<arrow_schema::Schema> {
        let schema = self.schema(tbl_name)?;
        Ok(schema2_to_schema1(schema))
    }

    pub fn get(&self, opts: ReadOptions, key: Vec<KeyValue>) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }

    pub fn delete(&self, opts: WriteOptions, key: Vec<KeyValue>) -> Result<()> {
        unimplemented!()
    }

    pub fn scan_partition(
        &self,
        tbl_name: &str,
        partition_id: usize,
        fields: Vec<String>,
    ) -> Result<ScanStream> {
        counter!("store.scans_count", 1, "table"=>tbl_name.to_string());
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let mut tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };

        drop(tables);
        // locking compactor file operations (deleting/moving) to prevent parts deletion during scanning
        let _vfs = tbl.vfs.lock.lock();
        let _md = tbl.metadata.lock();
        let metadata = _md.clone();
        drop(_md);
        let partition = metadata.partitions.iter().find(|p| p.id == partition_id).unwrap().to_owned();
        let start_time = Instant::now();
        let memtable = tbl.memtable.lock();
        let mem_chunks = memtable_to_partitioned_chunks(&metadata, &memtable, Some(partition_id))?;
        drop(memtable);
        histogram!("store.scan_memtable_time_sec",start_time.elapsed(),"table"=>tbl_name.to_string());
        let mut rdrs = vec![];
        let mem_chunk = mem_chunks
            .iter()
            .find(|c| c.partition_id == partition.id)
            .map(|c| c.chunk.clone());

        for (level_id, level) in partition.levels.into_iter().enumerate() {
            for part in level.parts {
                let path = part_path(&self.path, tbl_name, partition.id, level_id, part.id);
                let rdr = BufReader::new(File::open(path)?);
                rdrs.push(rdr);
            }
        }

        let iter = if rdrs.len() == 0 {
            Box::new(MemChunkIterator::new(mem_chunk))
                as Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>
        } else {
            let opts = arrow_merger::Options {
                index_cols: metadata.opts.merge_index_cols,
                array_size: metadata.opts.merge_array_size,
                fields: fields.clone(),
            };
            Box::new(MergingIterator::new(rdrs, mem_chunk, opts)?)
                as Box<dyn Iterator<Item=Result<Chunk<Box<dyn Array>>>> + Send>
        };

        Ok(ScanStream::new(iter, tbl_name.to_string()))
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn add_field(&self, tbl_name: &str, field_name: &str, dt: DType, is_nullable: bool) -> Result<()> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();
        drop(tables);

        let mut metadata = tbl.metadata.lock();
        for f in &metadata.schema.fields {
            if f.name == field_name {
                return Err(StoreError::Internal(format!(
                    "Field with name {} already exists",
                    field_name
                )));
            }
        }

        let field = Field::new(field_name.to_string(), dt.try_into()?, is_nullable);
        metadata.schema.fields.push(field);
        let mut log = tbl.log.lock();
        log_metadata(log.get_mut(), &mut metadata)?;
        Ok(())
    }

    pub fn create_table(&self, tbl_name: &str, opts: TableOptions) -> Result<()> {
        let path = self.path.join("tables").join(tbl_name);
        fs::create_dir_all(&path)?;
        let new_tbl = try_recover_table(path, tbl_name.to_string(), Some(opts))?;

        let mut tables = self.tables.write();
        tables.push(new_tbl);

        Ok(())
    }

    pub fn table_options(&self, tbl_name: &str) -> Result<TableOptions> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();
        drop(tables);
        let metadata = tbl.metadata.lock();
        Ok(metadata.opts.clone())
    }
}

impl Drop for OptiDBImpl {
    fn drop(&mut self) {
        let (tx, rx) = mpsc::channel();

        if self
            .compactor_outbox
            .send(CompactorMessage::Stop(tx))
            .is_err()
        {
            error!("failed to shut down compaction worker on database drop");
            return;
        }

        for _ in rx {}
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs;
    use std::io;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::Schema;
    use futures::executor::block_on;
    use futures::Stream;
    use futures::StreamExt;
    use rand::rngs::mock::StepRng;
    use rand::thread_rng;
    use rand::Rng;
    use shuffle::fy::FisherYates;
    use shuffle::irs::Irs;
    use shuffle::shuffler::Shuffler;
    use tracing::info;
    use tracing::log;
    use tracing::log::debug;
    use tracing::trace;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_test::traced_test;
    use common::types::DType;

    use crate::db::print_partitions;
    use crate::db::Level;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::db::Part;
    use crate::db::TableOptions;
    use crate::KeyValue;
    use crate::NamedValue;
    use crate::Value;

    #[traced_test]
    #[test]
    fn it_works() {
        // let path = temp_dir().join("db");
        let path = PathBuf::from(" / opt / homebrew / Caskroom / clickhouse / user_files");
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir_all(&path).unwrap();

        let opts = Options {};
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        let topts = TableOptions {
            levels: 7,
            merge_array_size: 10000,
            partitions: 2,
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
            read_chunk_size: 10,
        };

        // let topts = TableOptions {
        // levels: 7,
        // merge_array_size: 10000,
        // partitions: 2,
        // index_cols: 2,
        // l1_max_size_bytes: 1024 * 1024,
        // level_size_multiplier: 10,
        // l0_max_parts: 4,
        // max_log_length_bytes: 1024 * 1024,
        // merge_array_page_size: 10000,
        // merge_data_page_size_limit_bytes: Some(1024 * 1024),
        // merge_index_cols: 2,
        // merge_max_l1_part_size_bytes: 1024 * 1024,
        // merge_part_size_multiplier: 10,
        // merge_row_group_values_limit: 1000,
        // read_chunk_size: 10,
        // };

        let cols = 100;
        db.create_table("t1", topts).unwrap();
        for col in 0..cols {
            db.add_field("t1", col.to_string().as_str(), DType::Int64, false)
                .unwrap();
        }

        let mut rng = StepRng::new(2, 13);
        let mut irs = FisherYates::default();
        // let mut input = (0..10_000_000).collect();
        let mut input = (0..1000000).collect::<Vec<i64>>();
        irs.shuffle(&mut input, &mut rng).unwrap();

        for _ in 0..2 {
            for i in input.clone() {
                let vals = (0..cols)
                    .map(|v| NamedValue::new(v.to_string(), Value::Int64(Some(v as i64))))
                    .collect::<Vec<_>>();
                db.insert("t1", vals).unwrap();
            }
        }
        let names = (0..cols).map(|v| v.to_string()).collect::<Vec<_>>();
        let streams = db.scan_partition("t1", 2, names).unwrap();
        for stream in streams {
            let b = block_on(stream.collect::<Vec<_>>())
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>();

            // for bb in b {
            //     println!("{: ?}", bb);
            // }
        }
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        // print_partitions(db.tables.read()[0].metadata.lock().partitions.as_ref());
        // db.compact();
    }

    #[test]
    fn test_schema_evolution() {
        let path = PathBuf::from(" / opt / homebrew / Caskroom / clickhouse / user_files");
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir_all(&path).unwrap();

        let opts = Options {};
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        let topts = TableOptions {
            levels: 7,
            merge_array_size: 10000,
            partitions: 1,
            index_cols: 1,
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
        };

        db.create_table("t1", topts).unwrap();
        db.add_field("t1", "k1", DType::Int64, false)
            .unwrap();
        db.add_field("t1", "v1", DType::Int64, false)
            .unwrap();

        db.insert("t1", vec![NamedValue::new(
            "v1".to_string(),
            Value::Int64(Some(1)),
        )])
            .unwrap();

        db.add_field("t1", "v2", DType::Int64, false)
            .unwrap();

        db.insert("t1", vec![
            NamedValue::new("k1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("v1".to_string(), Value::Int64(Some(1))),
        ])
            .unwrap();

        db.flush().unwrap();
    }
}
