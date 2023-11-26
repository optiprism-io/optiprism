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
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::RwLock;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time;
use std::time::Instant;

use arrow::compute::concat;
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
use arrow2::datatypes::DataType;
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
use chrono::Duration;
use crossbeam_skiplist::SkipSet;
use futures::select;
use futures::Stream;
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
use crate::ColValue;
use crate::KeyValue;
use crate::RowValue;
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

#[derive(Clone)]
pub struct OptiDB {
    inner: Arc<OptiDBImpl>,
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
    pub read_chunk_size: usize,
    pub merge_max_part_size_bytes: usize,
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

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) {
        let _g = self.lock.lock().unwrap();
        fs::remove_file(path);
    }

    pub fn rename<P: AsRef<Path>>(&self, from: P, to: P) {
        let _g = self.lock.lock().unwrap();
        fs::rename(from, to);
    }
}

enum FsOp {
    Rename(PathBuf, PathBuf),
    Delete(PathBuf),
}
#[derive(Clone)]
struct Options {}
#[derive(Clone)]
pub struct OptiDBImpl {
    opts: Options,
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
        read_chunk_size: 0,
        merge_max_part_size_bytes: 0,
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
        let mut f = BufReader::new(File::open(&log_path)?);

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
            f.read_exact(&mut len_b)?;
            let len = u64::from_le_bytes(len_b);
            // !@#trace!("len: {}", len);
            let mut data_b = vec![0u8; len as usize];
            f.read_exact(&mut data_b)?;
            // todo recover from this case
            let cur_crc32 = hash_crc32(&data_b);
            if crc32 != cur_crc32 {
                return Err(StoreError::Internal(format!(
                    "corrupted log. crc32 has: {}, need: {}",
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
                .map(|(idx, col)| match metadata.schema.fields[idx].data_type {
                    DataType::Boolean => {
                        memory_col_to_arrow!(col, Boolean, MutableBooleanArray)
                    }
                    DataType::Int8 => memory_col_to_arrow!(col, Int8, MutablePrimitiveArray),
                    DataType::Int16 => memory_col_to_arrow!(col, Int16, MutablePrimitiveArray),
                    DataType::Int32 => memory_col_to_arrow!(col, Int32, MutablePrimitiveArray),
                    DataType::Int64 => memory_col_to_arrow!(col, Int64, MutablePrimitiveArray),
                    DataType::UInt8 => memory_col_to_arrow!(col, UInt8, MutablePrimitiveArray),
                    DataType::UInt16 => {
                        memory_col_to_arrow!(col, UInt16, MutablePrimitiveArray)
                    }
                    DataType::UInt32 => {
                        memory_col_to_arrow!(col, UInt32, MutablePrimitiveArray)
                    }
                    DataType::UInt64 => {
                        memory_col_to_arrow!(col, UInt64, MutablePrimitiveArray)
                    }
                    // DataType::Float32 => memory_col_to_arrow!(col, Float32, MutablePrimitiveArray),
                    // DataType::Float64 => memory_col_to_arrow!(col, Float64, MutablePrimitiveArray),
                    DataType::Timestamp(_, _) => {
                        memory_col_to_arrow!(col, Int64, MutablePrimitiveArray)
                    }
                    DataType::Binary => {
                        let vals = col
                            .into_iter()
                            .map(|v| match v {
                                Value::Binary(b) => b,
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>();
                        MutableBinaryArray::<i32>::from(vals).as_box()
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
                        memory_col_to_arrow!(col, Decimal, MutablePrimitiveArray)
                    }
                    DataType::List(_) => unimplemented!(),
                    _ => unimplemented!(),
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
    let partitioned_chunks = memtable_to_partitioned_chunks(metadata, memtable)?;
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
        let mut writer = parquet::write::FileWriter::try_new(w, metadata.schema.clone(), popts)?;
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
    Ok(())
}

pub struct ScanStream {
    iters: Vec<Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>>>>,
    iter_idx: usize,
}

impl ScanStream {
    pub fn new(iters: Vec<Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>>>>) -> Self {
        Self { iters, iter_idx: 0 }
    }
}

impl Stream for ScanStream {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let idx = self.iter_idx;
        match self.iters[idx].next() {
            None => {
                self.iter_idx += 1;
                if self.iter_idx >= self.iters.len() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Chunk::new(vec![]))))
                }
            }
            Some(chunk) => Poll::Ready(Some(chunk)),
        }
    }
}

struct EmptyStream {}

impl Stream for EmptyStream {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }

    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&mut self, tbl_name: &str, key: Vec<KeyValue>, values: Vec<Value>) -> Result<()> {
        let tables = self.tables.read().unwrap();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();

        drop(tables);

        let mut metadata = tbl.metadata.lock().unwrap();
        if key.len() + values.len() != metadata.schema.fields.len() {
            return Err(StoreError::Internal(format!(
                "Fields mismatch. Key+Val len: {}, schema fields len: {}",
                key.len() + values.len(),
                metadata.schema.fields.len()
            )));
        }
        let mut log = tbl.log.lock().unwrap();
        let logged = _log(
            LogOp::Insert(key.clone(), values.clone()),
            &mut metadata,
            log.get_mut(),
        )?;
        metadata.stats.logged_bytes += logged as u64;
        metadata.seq_id += 1;

        let mut memtable = tbl.memtable.lock().unwrap();

        memtable.insert(MemOp {
            op: Op::Insert(key, values),
            seq_id: metadata.seq_id,
        });

        if metadata.stats.logged_bytes as usize > metadata.opts.max_log_length_bytes {
            let mut log = tbl.log.lock().unwrap();
            flush(log.get_mut(), &mut memtable, &mut metadata, &self.path)?;

            // if md.levels[0].parts.len() > 0 && md.levels[0].parts.len() > self.opts.l0_max_parts {
            //     self.compactor_outbox
            //         .send(CompactorMessage::Compact)
            //         .unwrap();
            // }
        }

        // !@#debug!("insert finished in {:?}", duration);
        Ok(())
    }

    pub fn compact(&self) {
        self.compactor_outbox
            .send(CompactorMessage::Compact)
            .unwrap();
    }

    pub fn flush(&mut self) -> Result<()> {
        let mut tbls = self.tables.write().unwrap();
        for tbl in tbls.iter_mut() {
            let mut log = tbl.log.lock().unwrap();
            let mut memtable = tbl.memtable.lock().unwrap();
            let mut metadata = tbl.metadata.lock().unwrap();
            flush(log.get_mut(), &mut memtable, &mut metadata, &self.path)?;
        }

        Ok(())
    }

    pub fn schema(&self, tbl_name: &str) -> Result<Schema> {
        let tables = self.tables.read().unwrap();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let mut tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl,
        };

        let schema = tbl.metadata.lock().unwrap().schema.clone();
        Ok(schema)
    }

    pub fn get(&self, opts: ReadOptions, key: Vec<KeyValue>) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }

    pub fn delete(&self, opts: WriteOptions, key: Vec<KeyValue>) -> Result<()> {
        unimplemented!()
    }

    pub fn scan(
        &self,
        tbl_name: &str,
        required_partitions: usize,
        fields: Vec<String>,
    ) -> Result<Vec<ScanStream>> {
        let tables = self.tables.read().unwrap();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let mut tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };

        drop(tables);
        // locking compactor file operations (deleting/moving) to prevent parts deletion during scanning
        let _vfs = tbl.vfs.lock.lock().unwrap();
        let _md = tbl.metadata.lock().unwrap();
        let metadata = _md.clone();
        drop(_md);
        let mut all_partitions = metadata.partitions.clone();
        let mut out = vec![];
        for _ in 0..required_partitions {
            match all_partitions.pop() {
                None => out.push(vec![]),
                Some(v) => {
                    out.push(vec![v]);
                }
            }
        }

        if all_partitions.len() > 0 {
            let mut rest = out.pop().unwrap();
            let mut b = all_partitions.drain(..).collect();
            rest.append(&mut b);
            out.push(rest);
        }
        let memtable = tbl.memtable.lock().unwrap();
        let mem_chunks = memtable_to_partitioned_chunks(&metadata, &memtable)?;
        drop(memtable);

        let mut streams = vec![];
        for partitions in out {
            let mut iters = vec![];
            for partition in partitions {
                let mut rdrs = vec![];
                let mem_chunk = mem_chunks
                    .iter()
                    .find(|c| c.partition_id == partition.id)
                    .map(|c| c.chunk.clone());

                for (level_id, level) in partition.levels.into_iter().enumerate() {
                    for part in level.parts {
                        let path = part_path(&self.path, tbl_name, partition.id, level_id, part.id);
                        let rdr = File::open(path)?;
                        rdrs.push(rdr);
                    }
                }

                if rdrs.len() == 0 {
                    let iter = Box::new(MemChunkIterator::new(mem_chunk))
                        as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>>>;
                    iters.push(iter);
                } else {
                    let opts = arrow_merger::Options {
                        index_cols: metadata.opts.merge_index_cols,
                        array_size: metadata.opts.merge_array_size,
                        fields: fields.clone(),
                    };
                    let iter = Box::new(MergingIterator::new(rdrs, mem_chunk, opts)?)
                        as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>>>;
                    iters.push(iter);
                }
            }
            streams.push(ScanStream::new(iters));
        }

        Ok(streams)
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn add_field(&mut self, tbl_name: &str, field: Field) -> Result<()> {
        let tables = self.tables.read().unwrap();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();
        drop(tables);

        let mut metadata = tbl.metadata.lock().unwrap();
        for f in &metadata.schema.fields {
            if f.name == field.name {
                return Err(StoreError::Internal(format!(
                    "Field with name {} already exists",
                    field.name
                )));
            }
        }
        metadata.schema.fields.push(field);
        let mut log = tbl.log.lock().unwrap();
        log_metadata(log.get_mut(), &mut metadata)?;
        Ok(())
    }

    pub fn create_table(&mut self, tbl_name: &str, opts: TableOptions) -> Result<()> {
        let path = self.path.join("tables").join(tbl_name);
        fs::create_dir_all(&path)?;
        let new_tbl = try_recover_table(path, tbl_name.to_string(), Some(opts))?;

        let mut tables = self.tables.write().unwrap();
        tables.push(new_tbl);

        Ok(())
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

    use crate::db::print_partitions;
    use crate::db::Level;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::db::Part;
    use crate::db::TableOptions;
    use crate::ColValue;
    use crate::KeyValue;
    use crate::Value;

    #[traced_test]
    #[test]
    fn it_works() {
        // let path = temp_dir().join("db");
        let path = PathBuf::from("/opt/homebrew/Caskroom/clickhouse/user_files");
        fs::remove_dir_all(&path).unwrap();
        // fs::create_dir_all(&path).unwrap();

        let opts = Options {};
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        let topts = TableOptions {
            levels: 7,
            merge_array_size: 10,
            partitions: 2,
            index_cols: 1,
            l1_max_size_bytes: 1024 * 1024,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024 * 1024 * 10,
            merge_array_page_size: 10000,
            merge_data_page_size_limit_bytes: Some(1024 * 1024),
            merge_index_cols: 2,
            merge_max_part_size_bytes: 2048,
            merge_row_group_values_limit: 1000,
            read_chunk_size: 10,
        };
        db.create_table("t1", topts).unwrap();
        db.add_field("t1", Field::new("a", DataType::Int64, false))
            .unwrap();
        db.add_field("t1", Field::new("b", DataType::Int64, false))
            .unwrap();
        db.add_field("t1", Field::new("c", DataType::Int64, false))
            .unwrap();

        let mut rng = StepRng::new(2, 13);
        let mut irs = FisherYates::default();
        // let mut input = (0..10_000_000).collect();
        let mut input = (0..100).collect();
        irs.shuffle(&mut input, &mut rng).unwrap();

        for i in input {
            db.insert("t1", vec![KeyValue::Int64(i), KeyValue::Int64(i)], vec![
                Value::Int64(Some(i)),
            ])
            .unwrap();
        }

        let streams = db
            .scan("t1", 2, vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
            ])
            .unwrap();

        for stream in streams {
            let b = block_on(stream.collect::<Vec<_>>())
                .into_iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>();

            for bb in b {
                println!("{:?}", bb);
            }
        }
        thread::sleep(Duration::from_millis(20));
        // print_partitions(db.metadata.lock().unwrap().partitions.as_ref());
        // db.compact();
    }
}
