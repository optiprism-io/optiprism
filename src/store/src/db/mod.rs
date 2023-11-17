mod compaction;

use std::{cmp, io, time};
use shuffle::shuffler::Shuffler;
use shuffle::irs::Irs;
use rand::rngs::mock::StepRng;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::mem;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{mpsc, MutexGuard, RwLock};
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread;
use std::time::Instant;
use rand::Rng;
use arrow2::array::{Array, Int32Array, Int64Array};
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
use arrow::compute::concat;
use arrow_buffer::ToByteSlice;
use bincode::deserialize;
use bincode::serialize;
use chrono::Duration;
use crossbeam_skiplist::SkipSet;
use futures::{select, Stream};
use parquet2::metadata::FileMetaData;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip::SipHasher13;
use tracing::{debug, trace};
use tracing::error;
use tracing::instrument;
use tracing_subscriber::fmt::time::FormatTime;

use crate::error::Result;
use crate::error::StoreError;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::ColValue;
use crate::db::compaction::{Compactor, CompactorMessage};
use crate::KeyValue;
use crate::parquet::merger;
use crate::parquet::merger::{FileMergeOptions, merge, ParquetMerger};
use crate::RowValue;
use crate::Value;

const PARTS_DIR: &str = "parts";

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metadata {
    version: u64,
    seq_id: u64,
    log_id: u64,
    schema: Schema,
    partitions: Vec<Partition>,
    // levels->parts
    stats: Stats,
}

#[derive(Debug, Serialize, Deserialize, Hash)]
pub struct Insert {
    key: Vec<KeyValue>,
    values: Vec<ColValue>,
}

#[derive(Debug, Serialize, Deserialize, Hash)]
pub struct Delete {
    key: Vec<KeyValue>,
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

#[derive(Clone, Debug)]
pub struct Options {
    pub partitions: usize,
    pub partition_keys: Vec<usize>,
    pub l0_max_parts: usize,
    pub l1_max_size_bytes: usize,
    pub level_size_multiplier: usize,
    pub max_log_length_bytes: usize,
    pub read_chunk_size: usize,
    pub merge_max_part_size_bytes: usize,
    pub merge_index_cols: usize,
    pub merge_data_page_size_limit_bytes: Option<usize>,
    pub merge_row_group_values_limit: usize,
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

fn part_path(path: &PathBuf, partition_id: usize, level_id: usize, part_id: usize) -> PathBuf {
    path.join(format!("parts/{}/{}/{}.parquet", partition_id, level_id, part_id))
}

pub struct Vfs {
    // not RWLock because of possible deadlocks
    lock: Mutex<usize>,
}

impl Vfs {
    pub fn new() -> Self {
        Self {
            lock: Default::default(),
        }
    }

    pub fn add_readers(&self, v: usize) -> Result<()> {
        let mut lock = self.lock.lock().unwrap();
        *lock = *lock + v;

        Ok(())
    }

    pub fn remove_readers(&self, v: usize) -> Result<()> {
        let mut lock = self.lock.lock().unwrap();
        *lock = *lock - v;
        Ok(())
    }

    pub fn remove_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        loop {
            let a = *self.lock.lock().unwrap();
            if a == 0 {
                fs::remove_file(path)?;
                break;
            }
        }
        Ok(())
    }

    pub fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> Result<()> {
        loop {
            let a = *self.lock.lock().unwrap();
            if a == 0 {
                fs::rename(from, to)?;
                break;
            }
        }
        Ok(())
    }
}

enum FsOp {
    Rename(PathBuf, PathBuf),
    Delete(PathBuf),
}


#[derive(Clone)]
pub struct OptiDBImpl {
    opts: Options,
    // mutex is needed to lock write during reads
    memtable: Arc<Mutex<SkipSet<MemOp>>>,
    metadata: Arc<Mutex<Metadata>>,
    vfs: Arc<Vfs>,
    log: Arc<Mutex<BufWriter<File>>>,
    compactor_outbox: Sender<CompactorMessage>,
    path: PathBuf,
}

fn _log(op: &LogOp, metadata: &mut Metadata, log: &mut File) -> Result<()> {
    let data = serialize(op)?;

    let crc = hash_crc32(&data);
    // !@#trace!("crc32: {}", crc);
    log.write_all(&crc.to_le_bytes())?;
    log.write_all(&(data.len() as u64).to_le_bytes())?;
    log.write_all(&data)?;

    let logged_size = 8 + 4 + data.len();
    // !@#trace!("logged size: {}", logged_size);
    metadata.stats.logged_bytes += logged_size as u64;

    Ok(())
}

fn log_op(
    op: LogOp,
    recover: bool,
    log: Option<&mut File>,
    memtable: &mut SkipSet<MemOp>,
    metadata: &mut Metadata,
) -> Result<()> {
    if recover {
        // !@#trace!("recover op: {:?}", op);
    } else {
        // !@#trace!("log op: {:?}", op);
    }
    if !recover {
        let log = log.unwrap();
        _log(&op, metadata, log)?;
    }
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
    // !@#trace!("next op id: {}", metadata.seq_id);

    Ok(())
}

fn log_metadata(log: Option<&mut File>, metadata: &mut Metadata) -> Result<()> {
    // !@#trace!("log metadata");
    let log = log.unwrap();
    _log(&LogOp::Metadata(metadata.clone()), metadata, log)?;
    metadata.seq_id += 1;
    // !@#trace!("next op id: {}", metadata.seq_id);

    Ok(())
}

// #[instrument(level = "trace")]
fn recover(path: PathBuf, opts: Options) -> Result<OptiDBImpl> {
    // !@#trace!("db dir: {:?}", path);
    for l in 0..opts.partitions {
        for i in 0..7 {
            fs::create_dir_all(path.join("parts").join(l.to_string()).join(i.to_string()))?;
        }
    }
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

    let mut metadata = Metadata {
        version: 0,
        seq_id: 0,
        log_id: 0,
        schema: Schema::from(vec![]),
        stats: Default::default(),
        partitions: (0..opts.partitions).into_iter().map(|pid| Partition { id: pid, levels: vec![Level::new_empty(); 7] }).collect::<Vec<_>>(),
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
            log_op(op, true, None, &mut memtable, &mut metadata)?;

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
    /*let trigger_compact = if metadata.partitions[0].parts.len() > 0
        && metadata.partitions[0].parts.len() > opts.l0_max_parts
    {
        true
    } else {
        false
    };*/
    let (compactor_outbox, rx) = std::sync::mpsc::channel();
    let metadata = Arc::new(Mutex::new(metadata));
    let log = Arc::new(Mutex::new(BufWriter::new(log)));
    let vfs = Arc::new(Vfs::new());
    let compactor = Compactor::new(opts.clone(), metadata.clone(), log.clone(), path.clone(), rx, vfs.clone());
    thread::spawn(move || compactor.run());
    // if trigger_compact {
    //     // compactor_outbox.send(CompactorMessage::Compact).unwrap();
    // }
    Ok(OptiDBImpl {
        opts,
        memtable: Arc::new(Mutex::new(memtable)),
        metadata,
        vfs,
        log,
        compactor_outbox,
        path,
    })
}

#[derive(Debug)]
struct MemtablePartitionChunk {
    partition_id: usize,
    chunk: Chunk<Box<dyn Array>>,
    min: Vec<KeyValue>,
    max: Vec<KeyValue>,
}

#[derive(Debug)]
struct MemtablePartition {
    id: usize,
    min: Option<Vec<KeyValue>>,
    max: Option<Vec<KeyValue>>,
    cols: Vec<Vec<Value>>,
}

impl MemtablePartition {
    pub fn new(id: usize, fields: usize) -> Self {
        Self {
            id,
            min: None,
            max: None,
            cols: vec![Vec::new(); fields],
        }
    }
}

fn memtable_to_partitioned_chunks(memtable: &SkipSet<MemOp>,
                                  schema: &Schema, partitions_count: usize, partition_keys: Vec<usize>) -> Result<Vec<MemtablePartitionChunk>> {
    let mut partitions: Vec<MemtablePartition> = vec![];
    // let mut partitions = vec![MemtablePartition::new(schema.fields.len()); partition_count];
// !@#trace!("{} entries to write", memtable.len());
    for op in memtable.iter() {
        let (keys, vals) = match &op.op {
            Op::Insert(k, v) => (k, Some(v)),
            Op::Delete(k) => (k, None),
        };
        let pid_keys = partition_keys.iter().map(|pk| keys[*pk].clone()).collect::<Vec<_>>();
        let phash = siphash(&pid_keys);
        let mut pid: isize = -1;
        for (pidx, p) in partitions.iter().enumerate() {
            if p.id == phash as usize % partitions_count {
                pid = pidx as isize;
                break;
            }
            continue;
        }
        if pid < 0 {
            partitions.push(MemtablePartition::new(phash as usize % partitions_count, schema.fields.len()));
            pid = 0;
        }
        let pid = pid as usize;
        if partitions[pid].min.is_none() {
            partitions[pid].min = Some(keys.clone());
        }
        partitions[pid].max = Some(keys.clone());
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
    let v = partitions.into_iter().map(|p| {
        // println!("{:?}", p);
        let arrs = p.cols
            .into_iter()
            .enumerate()
            .map(|(idx, col)| match schema.fields[idx].data_type {
                DataType::Boolean => memory_col_to_arrow!(col, Boolean, MutableBooleanArray),
                DataType::Int8 => memory_col_to_arrow!(col, Int8, MutablePrimitiveArray),
                DataType::Int16 => memory_col_to_arrow!(col, Int16, MutablePrimitiveArray),
                DataType::Int32 => memory_col_to_arrow!(col, Int32, MutablePrimitiveArray),
                DataType::Int64 => memory_col_to_arrow!(col, Int64, MutablePrimitiveArray),
                DataType::UInt8 => memory_col_to_arrow!(col, UInt8, MutablePrimitiveArray),
                DataType::UInt16 => memory_col_to_arrow!(col, UInt16, MutablePrimitiveArray),
                DataType::UInt32 => memory_col_to_arrow!(col, UInt32, MutablePrimitiveArray),
                DataType::UInt64 => memory_col_to_arrow!(col, UInt64, MutablePrimitiveArray),
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
        (p.id, Chunk::new(arrs), p.min.clone().unwrap(), p.max.clone().unwrap())
    }).collect::<Vec<_>>();


    let chunks = v.into_iter().map(|(pid, chunk, min, max)| {
        MemtablePartitionChunk {
            partition_id: pid,
            chunk,
            min,
            max,
        }
    }).collect::<Vec<_>>();

    Ok(chunks)
}


fn write_level0(
    memtable: &SkipSet<MemOp>,
    partitions: &[Partition],
    partition_keys: Vec<usize>,
    schema: &Schema,
    path: PathBuf,
) -> Result<Vec<(usize, Part)>> {
    let mut ret = vec![];
    let partitioned_chunks = memtable_to_partitioned_chunks(memtable, schema, partitions.len(), partition_keys)?;
    for chunk in partitioned_chunks.into_iter() {
        let popts = parquet::write::WriteOptions {
            write_statistics: true,
            compression: parquet::write::CompressionOptions::Snappy,
            version: parquet::write::Version::V1,
            //todo define page size
            data_pagesize_limit: None,
        };

        let encodings = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| parquet::write::Encoding::Plain))
            .collect();
        let row_groups = parquet::write::RowGroupIterator::try_new(
            vec![Ok(chunk.chunk)].into_iter(),
            schema,
            popts,
            encodings,
        )?;

        let path = part_path(&path, chunk.partition_id, 0, partitions[chunk.partition_id].levels[0].part_id);
        // !@#trace!("creating part file {:?}", p);
        println!("{:?}", path);
        let w = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut writer = parquet::write::FileWriter::try_new(w, schema.clone(), popts)?;
        // Write the part
        for group in row_groups {
            writer.write(group?)?;
        }
        let sz = writer.end(None)?;
        ret.push((chunk.partition_id, Part {
            id: partitions[chunk.partition_id].levels[0].part_id,
            size_bytes: sz,
            min: chunk.min,
            max: chunk.max,
        }))
    }
    // !@#trace!("{:?} bytes written", sz);
    Ok(ret)
}

fn flush(
    log: Option<&mut File>,
    memtable: &mut SkipSet<MemOp>,
    md: &mut Metadata,
    partition_keys: Vec<usize>,
    path: &PathBuf,
) -> Result<()> {
    // in case when it is flushed by another thread
    if memtable.len() == 0 {
        return Ok(());
    }
    // !@#trace!("flushing log file");
    let f = log.unwrap();
    f.flush()?;
    f.sync_all()?;
    // swap memtable
    // !@#trace!("swapping memtable");
    let mut memtable = std::mem::take(memtable);

    // write to parquet
    // !@#trace!("writing to parquet");
    // !@#trace!("part id: {}", md.levels[0].part_id);
    let parts = write_level0(&memtable, &md.partitions, partition_keys, &md.schema, path.clone())?;
    for (pid, part) in parts {
        md.partitions[pid].levels[0].parts.push(part.clone());
        // increment table id
        md.partitions[pid].levels[0].part_id += 1;
        md.stats.written_bytes += part.size_bytes;
    }

    md.stats.logged_bytes = 0;
    // increment log id
    md.log_id += 1;

    // create new log
    trace!(
        "creating new log file {:?}",
        format!("{:016}.log", md.log_id)
    );

    let log = OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path.join(format!("{:016}.log", md.log_id)))?;
    let mut log = BufWriter::new(log);

    // write metadata to log as first record
    log_metadata(Some(log.get_mut()), md)?;

    trace!(
        "removing previous log file {:?}",
        format!("{:016}.log", md.log_id - 1)
    );
    fs::remove_file(path.join(format!("{:016}.log", md.log_id - 1)))?;
    Ok(())
}

pub struct IterateOptions {
    fields: Vec<Field>,
}

#[derive(Debug, Clone)]
struct DiskPart {
    id: usize,
    level: usize,
    min: Vec<KeyValue>,
    max: Vec<KeyValue>,
}

impl PartialOrd for DiskPart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DiskPart {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min
    }
}

impl Ord for DiskPart {
    fn cmp(&self, other: &Self) -> Ordering {
        self.min.partial_cmp(&other.min).unwrap()
    }
}

impl Eq for DiskPart {}

#[derive(Debug, Clone)]
struct MemPart {
    chunk: Chunk<Box<dyn Array>>,
    min: Vec<KeyValue>,
    max: Vec<KeyValue>,
}

impl PartialOrd for MemPart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MemPart {
    fn eq(&self, other: &Self) -> bool {
        self.min == other.min
    }
}

impl Ord for MemPart {
    fn cmp(&self, other: &Self) -> Ordering {
        self.min.partial_cmp(&other.min).unwrap()
    }
}

impl Eq for MemPart {}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
enum StreamPart {
    Mem(MemPart),
    Disk(DiskPart), // level, part
}

pub struct PartitionStream {
    vfs: Arc<Vfs>,
    levels: Vec<Level>,
    cur_level: usize,
    cur_part: usize,
    memtable_chunk: Option<Chunk<Box<dyn Array>>>,
    file_md: FileMetaData,
    path: PathBuf,
    fields: Vec<Field>,
    rdr: FileReader<File>,
    chunk_size: usize,
    sorted: BinaryHeap<StreamPart>,
}

#[derive(Debug, Clone)]
pub struct RetStream {
    l: usize,
}

impl RetStream {
    pub fn new(partitions: Vec<Partition>) -> Self {
        Self { l: partitions.len() }
    }
}

impl Stream for RetStream {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.l == 0 {
            return Poll::Ready(None);
        }

        self.l -= 1;
        let a1 = Int64Array::from(vec![Some(1), Some(1), Some(2), Some(2), Some(3)]);
        let a2 = Int64Array::from(vec![Some(1), Some(2), Some(1), Some(2), Some(1)]);
        let a3 = Int64Array::from(vec![Some(1), Some(2), Some(1), Some(2), Some(1)]);

        Poll::Ready(Some(Ok(Chunk::new(vec![a1.boxed(), a2.boxed(), a3.boxed()]))))
    }
}

struct EmptyStream {}

impl Stream for EmptyStream {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
/*impl PartitionStream {
    pub fn try_new(partitions: Vec<(Partition, Option<MemtablePartitionChunk>)>, chunk_size: usize, vfs: Arc<Vfs>, path: PathBuf, fields: Vec<Field>) -> Result<Self> {
        Ok(Self {
            vfs,
            levels: vec![],
            cur_level: 0,
            cur_part: 0,
            memtable_chunk: None,
            file_md: FileMetaData {},
            path,
            fields,
            rdr: (),
            chunk_size,
            sorted: Default::default(),
        })
        /*let mut sorted: BinaryHeap<StreamPart> = Default::default();
        if let Some(chunk) = memtable_chunk {
            sorted.push(StreamPart::Mem(MemPart {
                chunk: chunk.chunk,
                min: chunk.min,
                max: chunk.max,
            }))
        }

        for (lid, l) in levels.iter().enumerate() {
            for p in l.parts {
                let disk_part = DiskPart {
                    id: p.id,
                    level: lid,
                    min: p.min,
                    max: p.max,
                };
                sorted.push(StreamPart::Disk(disk_part));
            }
        }

        for p in sorted.iter() {
            println!("{:?}", p);
        }
*/
    }
}*/

impl Stream for PartitionStream {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

pub struct PartitionIterator {
    vfs: Arc<Vfs>,
    levels: Vec<Level>,
    cur_level: usize,
    cur_part: usize,
    memtable_chunk: Option<Chunk<Box<dyn Array>>>,
    file_md: FileMetaData,
    path: PathBuf,
    fields: Vec<Field>,
    rdr: FileReader<File>,
    chunk_size: usize,
}

impl PartitionIterator {
    pub fn new(levels: Vec<Level>, memtable_chunk: Chunk<Box<dyn Array>>, chunk_size: usize, vfs: Arc<Vfs>, path: PathBuf, fields: Vec<Field>) -> Result<Self> {
        let mut level_idx = 0;
        let mut part_idx = 0;

        // get first part
        let mut found = false;
        'outer: for (lid, level) in levels.iter().enumerate() {
            for (pid, part) in level.parts.iter().enumerate() {
                found = true;
                level_idx = lid;
                part_idx = pid;
                break 'outer;
            }
        }
        if !found {}
        println!("{:?}", path.join(format!("parts/{level_idx}/{part_idx}.parquet")));
        let mut rdr = File::open(path.join(format!("parts/{level_idx}/{part_idx}.parquet")))?;
        println!("!");

        let file_md = read::read_metadata(&mut rdr)?;
        let schema = read::infer_schema(&file_md)?;
        let schema = schema.filter(|_, field| fields.contains(field));

        let rdr = read::FileReader::new(rdr, file_md.row_groups.clone(), schema, Some(1024 * 8 * 8), None, None);

        Ok(Self {
            vfs,
            levels,
            cur_level: level_idx,
            cur_part: part_idx,
            memtable_chunk: Some(memtable_chunk),
            file_md,
            path,
            fields,
            rdr,
            chunk_size,
        })
    }
}

impl Iterator for PartitionIterator {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(chunk) = self.memtable_chunk.take() {
            return Some(Ok(chunk));
        }

        loop {
            let maybe_chunk = self.rdr.next();
            if let Some(chunk) = maybe_chunk {
                return Some(chunk.map_err(|e| e.into()));
            }

            loop {
                self.cur_part = self.cur_part + 1;
                match self.levels.get(self.cur_level) {
                    None => return None,
                    Some(level) => match level.parts.get(self.cur_part) {
                        None => self.cur_level += 1,
                        Some(_) => {
                            break;
                        }
                    }
                }
            }

            let rdr = File::open(self.path.join(format!("parts/{}/{}.parquet", self.cur_level, self.cur_part)));
            // todo is there a better way?
            let mut rdr = match rdr {
                Ok(rdr) => rdr,
                Err(err) => return Some(Err(err.into()))
            };

            let file_md = read::read_metadata(&mut rdr);
            let file_md = match file_md {
                Ok(file_md) => file_md,
                Err(err) => return Some(Err(err.into()))
            };
            let schema = read::infer_schema(&file_md);
            let schema = match schema {
                Ok(schema) => schema,
                Err(err) => return Some(Err(err.into()))
            };
            let schema = schema.filter(|_, field| self.fields.contains(field));

            self.rdr = FileReader::new(rdr, file_md.row_groups.clone(), schema, Some(self.chunk_size), None, None);
        }
    }
}

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&mut self, key: Vec<KeyValue>, values: Vec<Value>) -> Result<()> {
        let start = Instant::now();
        let mut md = self.metadata.lock().unwrap();
        if key.len() + values.len() != md.schema.fields.len() {
            return Err(StoreError::Internal(format!(
                "Fields mismatch. Key+Val len: {}, schema fields len: {}",
                key.len() + values.len(),
                md.schema.fields.len()
            )));
        }
        let mut memtable = self.memtable.lock().unwrap();
        log_op(
            LogOp::Insert(key, values),
            false,
            Some(self.log.lock().unwrap().get_mut()),
            &mut memtable,
            &mut md,
        )?;
        if md.stats.logged_bytes as usize > self.opts.max_log_length_bytes {
            flush(
                Some(self.log.lock().unwrap().get_mut()),
                &mut memtable,
                &mut md,
                self.opts.partition_keys.clone(),
                &self.path,
            )?;

            // if md.levels[0].parts.len() > 0 && md.levels[0].parts.len() > self.opts.l0_max_parts {
            //     self.compactor_outbox
            //         .send(CompactorMessage::Compact)
            //         .unwrap();
            // }
        }

        let duration = start.elapsed();
        // !@#debug!("insert finished in {:?}", duration);
        Ok(())
    }

    pub fn compact(&self) {
        self.compactor_outbox
            .send(CompactorMessage::Compact)
            .unwrap();
    }
    pub fn flush(&mut self) -> Result<()> {
        let mut md = self.metadata.lock().unwrap();
        let mut memtable = self.memtable.lock().unwrap();
        flush(
            Some(self.log.lock().unwrap().get_mut()),
            &mut memtable,
            &mut md,
            self.opts.partition_keys.clone(),
            &self.path,
        )?;

        Ok(())
    }

    pub fn schema(&self) -> Schema {
        self.metadata.lock().unwrap().schema.clone()
    }

    pub fn get(&self, opts: ReadOptions, key: Vec<KeyValue>) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }

    pub fn delete(&self, opts: WriteOptions, key: Vec<KeyValue>) -> Result<()> {
        unimplemented!()
    }

    pub fn scan(&self, required_partitions: usize, fields: Vec<Field>) -> Result<Vec<RetStream>> {
        let md = self.metadata.lock().unwrap();
        let schema = md.schema.clone();
        let mut partitions = md.partitions.clone();
        drop(md);
        let mut mt = self.memtable.lock().unwrap();
        let chunk = memtable_to_partitioned_chunks(
            &mut mt,
            &schema,
            self.opts.partitions,
            self.opts.partition_keys.clone(),
        )?;

        let mut out = vec![];
        for _ in 0..required_partitions {
            match partitions.pop() {
                None => out.push(vec![]),
                Some(v) => {
                    out.push(vec![v]);
                }
            }
        }

        if partitions.len() > 0 {
            let mut rest = out.pop().unwrap();
            let mut b = partitions.drain(..).collect();
            rest.append(&mut b);
            out.push(rest);
        }

        let streams = out.into_iter().map(|parts| RetStream::new(parts)).collect::<Vec<_>>();

        Ok(streams)
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn add_field(&mut self, field: Field) -> Result<()> {
        let mut md = self.metadata.lock().unwrap();
        for f in &md.schema.fields {
            if f.name == field.name {
                return Err(StoreError::Internal(format!(
                    "Field with name {} already exists",
                    field.name
                )));
            }
        }
        md.schema.fields.push(field);
        log_metadata(Some(self.log.lock().unwrap().get_mut()), &mut md)?;
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
    use std::{fs, io, thread};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::Schema;
    use rand::{Rng, thread_rng};
    use rand::rngs::mock::StepRng;
    use shuffle::fy::FisherYates;
    use shuffle::irs::Irs;
    use shuffle::shuffler::Shuffler;
    use tracing::info;
    use tracing::log;
    use tracing::log::debug;
    use tracing::trace;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_test::traced_test;

    use crate::db::{IterateOptions, print_partitions};
    use crate::db::Insert;
    use crate::db::Level;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::db::Part;
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

        let opts = Options {
            partitions: 2,
            partition_keys: vec![0],
            l1_max_size_bytes: 1024 * 1024 * 10,
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
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        db.add_field(Field::new("a", DataType::Int64, false));
        db.add_field(Field::new("b", DataType::Int64, false));
        db.add_field(Field::new("c", DataType::Int64, false));

        let mut rng = StepRng::new(2, 13);
        let mut irs = FisherYates::default();
        // let mut input = (0..10_000_000).collect();
        let mut input = (0..1000000).collect();
        irs.shuffle(&mut input, &mut rng).unwrap();

        for i in input {
            db.insert(
                vec![
                    KeyValue::Int64(i),
                    KeyValue::Int64(i),
                ],
                vec![Value::Int64(Some(i))],
            )
                .unwrap();
        }

        let opts = IterateOptions {
            fields: vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
            ]
        };

        // for chunk in db.iterate(opts).unwrap() {
        //     let chunk = chunk.unwrap();
        //     println!("{:?}", chunk);
        // }
        db.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        print_partitions(db.metadata.lock().unwrap().partitions.as_ref());
        // db.compact();
    }
}