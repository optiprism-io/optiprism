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
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time;
use std::time::Duration;
use std::time::Instant;

use arrow::compute::concat;
use arrow::ipc::Time;
use arrow2::array::clone;
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
use arrow2::compute::merge_sort::SortOptions;
use arrow2::compute::sort::lexsort_to_indices;
use arrow2::compute::sort::SortColumn;
use arrow2::compute::take;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::datatypes::SchemaRef;
use arrow2::datatypes::TimeUnit;
use arrow2::io::parquet;
use arrow2::io::parquet::read;
use arrow2::io::parquet::read::FileReader;
use arrow2::io::parquet::write::transverse;
use arrow2::io::print;
use arrow_buffer::ToByteSlice;
use bincode::deserialize;
use bincode::serialize;
use chrono::format::Item;
use common::types::DType;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use crossbeam_skiplist::SkipSet;
use futures::select;
use futures::SinkExt;
use futures::Stream;
use get_size::GetSize;
use lazy_static::lazy_static;
use log::info;
use metrics::counter;
use metrics::describe_counter;
use metrics::describe_histogram;
use metrics::gauge;
use metrics::histogram;
use metrics::Counter;
use metrics::Gauge;
use metrics::Histogram;
use metrics::Unit;
use num_traits::ToPrimitive;
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

fn collect_metrics(tables: Arc<RwLock<Vec<Table>>>) {
    loop {
        let tables = tables.read();
        let tbl = tables.clone();
        drop(tables);

        for t in tbl {
            let md_mx = t.metadata.lock();
            let md = md_mx.clone();
            drop(md_mx);

            let mem_mx = t.memtable.lock();
            let sz = mem_mx.partitions.iter().map(|v| v.len()).sum::<usize>();
            histogram!("store.memtable_rows","table"=>t.name.to_string())
                .record(sz.to_f64().unwrap());
            drop(mem_mx);

            gauge!("store.table_fields", "table"=>t.name.to_string())
                .set(md.schema.fields.len().to_f64().unwrap());
            gauge!("store.sequence","table"=>t.name.to_string()).set(md.seq_id.to_f64().unwrap());
            for (pid, partition) in md.partitions.iter().enumerate() {
                for (lvlid, lvl) in partition.levels.iter().enumerate() {
                    let sz = lvl.parts.iter().map(|v| v.size_bytes).sum::<u64>();
                    let values = lvl.parts.iter().map(|v| v.values).sum::<usize>();
                    gauge!("store.parts_size_bytes","table"=>t.name.to_string(),"level"=>lvlid.to_string()).set(sz.to_f64().unwrap());
                    for part in lvl.parts.iter() {
                        // trace
                        histogram!("store.part_size_bytes","table"=>t.name.to_string(),"level"=>lvlid.to_string()).record(part.size_bytes.to_f64().unwrap());
                        histogram!("store.part_values","table"=>t.name.to_string(),"level"=>lvlid.to_string()).record(part.values.to_f64().unwrap());
                    }
                    gauge!("store.parts","table"=>t.name.to_string(),"level"=>lvlid.to_string())
                        .set(lvl.parts.len().to_f64().unwrap());
                    gauge!("store.parts_values","table"=>t.name.to_string(),"level"=>lvlid.to_string()).set(values.to_f64().unwrap());
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

macro_rules! memory_col_to_arrow {
    ($col:expr, $dt:ident,$arr_ty:ident) => {{
        let vals = $col
            .into_iter()
            .map(|v| match v {
                Value::$dt(b) => b,
                Value::Null => None,
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
    values: usize,
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
struct MemtableColumn {
    values: Vec<Value>,
    dt: DType,
}

impl MemtableColumn {
    fn new_null(len: usize, dt: DType) -> Self {
        Self {
            values: vec![Value::Null; len],
            dt,
        }
    }

    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            dt: self.dt.clone(),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn to_array(self) -> Box<dyn Array> {
        match self.dt.try_into().unwrap() {
            DataType::Boolean => {
                memory_col_to_arrow!(self.values, Boolean, MutableBooleanArray)
            }
            DataType::Int8 => memory_col_to_arrow!(self.values, Int8, MutablePrimitiveArray),
            DataType::Int16 => memory_col_to_arrow!(self.values, Int16, MutablePrimitiveArray),
            DataType::Int32 => memory_col_to_arrow!(self.values, Int32, MutablePrimitiveArray),
            DataType::Int64 => memory_col_to_arrow!(self.values, Int64, MutablePrimitiveArray),
            DataType::Timestamp(_, _) => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::Timestamp(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutablePrimitiveArray::<i64>::from(vals)
                    .to(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    .as_box()
            }
            DataType::Utf8 => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::String(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutableUtf8Array::<i32>::from(vals).as_box()
            }
            DataType::Decimal(_, _) => {
                let vals = self
                    .values
                    .into_iter()
                    .map(|v| match v {
                        Value::Decimal(b) => b,
                        Value::Null => None,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutablePrimitiveArray::<i128>::from(vals)
                    .to(DataType::Decimal(
                        DECIMAL_PRECISION as usize,
                        DECIMAL_SCALE as usize,
                    ))
                    .as_box()
            }
            DataType::List(_) => unimplemented!(),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
struct MemtablePartition {
    cols: Vec<MemtableColumn>,
}

impl MemtablePartition {
    fn new() -> Self {
        MemtablePartition { cols: vec![] }
    }
    fn len(&self) -> usize {
        if self.cols.len() == 0 {
            return 0;
        }
        self.cols[0].len()
    }

    fn chunk(
        &self,
        cols: Option<Vec<usize>>,
        index_cols: usize,
    ) -> Result<Option<Chunk<Box<dyn Array>>>> {
        if self.len() == 0 {
            return Ok(None);
        }

        let arrs = match cols {
            None => self
                .cols
                .iter()
                .map(|c| c.clone().to_array())
                .collect::<Vec<_>>(),
            Some(cols) => self
                .cols
                .iter()
                .enumerate()
                .filter(|(idx, _)| cols.contains(idx))
                .map(|(_, c)| c.clone().to_array())
                .collect::<Vec<_>>(),
        };

        let sort_cols = (0..index_cols)
            .into_iter()
            .map(|v| SortColumn {
                values: arrs[v].as_ref(),
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            })
            .collect::<Vec<_>>();

        let indices = lexsort_to_indices::<i32>(&sort_cols, None)?;

        let arrs = arrs
            .iter()
            .map(|arr| take::take(arr.as_ref(), &indices))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let chunk = Chunk::new(arrs);
        Ok(Some(chunk))
    }

    fn push_value(&mut self, col: usize, val: Value) {
        self.cols[col].values.push(val);
    }

    fn add_column(&mut self, dt: DType) {
        self.cols.push(MemtableColumn::new_null(self.len(), dt));
    }
}
#[derive(Debug, Clone)]
struct Memtable {
    partitions: Vec<MemtablePartition>,
}

impl Memtable {
    fn new(partitions: usize) -> Self {
        Self {
            partitions: vec![MemtablePartition::new(); partitions],
        }
    }
    fn cols_len(&self) -> usize {
        self.partitions[0].cols.len()
    }

    fn add_column(&mut self, dt: DType) {
        for partition in &mut self.partitions {
            partition.add_column(dt.clone());
        }
    }
    fn create_empty(&self) -> Self {
        let partitions = self
            .partitions
            .iter()
            .map(|p| MemtablePartition {
                cols: p
                    .cols
                    .iter()
                    .map(|c| MemtableColumn::new_null(0, c.dt.clone()))
                    .collect::<Vec<_>>(),
            })
            .collect::<Vec<_>>();
        Memtable { partitions }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    memtable: Arc<Mutex<Memtable>>,
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

#[derive(Debug, Serialize, Deserialize)]
pub enum LogOp {
    Insert(Vec<KeyValue>, Vec<Value>),
    Metadata(Metadata),
    AddField(Field),
}

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
    pub merge_chunk_size: usize,
    pub merge_array_page_size: usize,
    pub merge_max_page_size: usize,
}

impl TableOptions {
    pub fn test() -> Self {
        TableOptions {
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
            merge_chunk_size: 1024 * 8 * 8,
            merge_max_page_size: 1024 * 1024,
        }
    }
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

fn log_op(op: LogOp, log: &mut File) -> Result<usize> {
    let data = serialize(&op)?;

    let mut a = Vec::with_capacity(1024 * 10);
    let crc = hash_crc32(&data);
    let vv = crc.to_le_bytes();
    a.push(vv.as_slice());
    let vv = (data.len() as u64).to_le_bytes();
    a.push(vv.as_slice());
    a.push(&data);
    log.write_all(a.concat().as_slice())?;
    let logged_size = 8 + 4 + data.len();
    // !@#trace!("logged size: {}", logged_size);
    // log.flush()?;
    Ok(logged_size)
}

fn recover_op(op: LogOp, memtable: &mut Memtable, metadata: &mut Metadata) -> Result<()> {
    match op {
        LogOp::Insert(k, v) => {
            let phash = siphash(&k);
            let pid = phash as usize % metadata.opts.partitions;
            let kv: Vec<Value> = k.iter().map(|k| k.into()).collect::<Vec<_>>();
            let vv = [kv, v].concat();
            for (idx, val) in vv.into_iter().enumerate() {
                memtable.partitions[pid].push_value(idx, val);
            }
        }
        LogOp::AddField(f) => {
            metadata.schema.fields.push(f.clone());
            memtable.add_column(f.data_type.try_into()?);
        }
        LogOp::Metadata(md) => {
            *metadata = md;
        }
    }

    metadata.seq_id += 1;

    Ok(())
}

fn log_metadata(log: &mut File, metadata: &mut Metadata) -> Result<()> {
    // !@#trace!("log metadata");
    log_op(LogOp::Metadata(metadata.clone()), log)?;
    metadata.seq_id += 1;

    Ok(())
}

fn try_recover_table(path: PathBuf, name: String) -> Result<Table> {
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

    if logs.len() == 0 {
        return Err(StoreError::Internal("no logs found".to_string()));
    }

    // !@#trace!("found {} logs", logs.len());

    let log_path = logs.pop().unwrap();
    // !@#trace!("last log: {:?}", log_path);
    // todo make a buffered read. Currently something odd happens with reading the length of the record
    let mut log = OpenOptions::new().read(true).write(true).open(&log_path)?;
    let mut ops = 0;
    let mut read_bytes = 0;
    // get metadata (first record of the log)

    let mut crc_b = [0u8; mem::size_of::<u32>()];
    read_bytes = log.read(&mut crc_b)?;
    if read_bytes == 0 {
        return Err(StoreError::Internal("empty log file".to_string()));
    }
    let crc32 = u32::from_le_bytes(crc_b);
    // !@#trace!("crc32: {}", crc32);

    let mut len_b = [0u8; mem::size_of::<u64>()];
    log.read(&mut len_b)?;
    let len = u64::from_le_bytes(len_b);
    // !@#trace!("len: {}", len);
    let mut data_b = vec![0u8; len as usize];
    log.read(&mut data_b)?;
    // todo recover from this case
    let cur_crc32 = hash_crc32(&data_b);
    if crc32 != cur_crc32 {
        return Err(StoreError::Internal(format!(
            "corrupted log. crc32 is: {}, need: {}",
            cur_crc32, crc32
        )));
    }
    let op = deserialize::<LogOp>(&data_b)?;
    let mut metadata = match op {
        LogOp::Metadata(md) => md,
        _ => {
            return Err(StoreError::Internal(
                "first record in log must be metadata".to_string(),
            ));
        }
    };

    let mut memtable = Memtable::new(metadata.opts.partitions);

    for f in &metadata.schema.fields {
        memtable.add_column(f.data_type.clone().try_into()?);
    }
    loop {
        let mut crc_b = [0u8; mem::size_of::<u32>()];
        read_bytes = log.read(&mut crc_b)?;
        if read_bytes == 0 {
            break;
        }
        let crc32 = u32::from_le_bytes(crc_b);
        // !@#trace!("crc32: {}", crc32);

        let mut len_b = [0u8; mem::size_of::<u64>()];
        log.read(&mut len_b)?;
        let len = u64::from_le_bytes(len_b);
        // !@#trace!("len: {}", len);
        let mut data_b = vec![0u8; len as usize];
        log.read(&mut data_b)?;
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

    metadata.stats.logged_bytes = log_path.metadata().unwrap().len();

    let log = BufWriter::new(log);
    // if trigger_compact {
    //     // compactor_outbox.send(CompactorMessage::Compact).unwrap();
    // }
    histogram!("store.recovery_time_seconds").record(start_time.elapsed());
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
    let tables_cloned = tables.clone();
    thread::spawn(move || collect_metrics(tables_cloned));
    Ok(OptiDBImpl {
        opts,
        tables,
        compactor_outbox,
        path,
    })
}
// #[derive(Debug)]
// struct MemtablePartitionChunk {
// partition_id: usize,
// chunk: Chunk<Box<dyn Array>>,
// }
//
// #[derive(Debug)]
// struct MemtablePartition {
// id: usize,
// cols: Vec<Vec<Value>>,
// }
//
// impl MemtablePartition {
// pub fn new(id: usize, fields: usize) -> Self {
// Self {
// id,
// cols: vec![Vec::new(); fields],
// }
// }
// }

fn write_level0(
    metadata: &Metadata,
    memtable: &Memtable,
    path: PathBuf,
) -> Result<Vec<(usize, Part)>> {
    let mut ret = vec![];
    for (pid, partition) in memtable.partitions.iter().enumerate() {
        let maybe_chunk = partition.chunk(None, metadata.opts.index_cols)?;
        if maybe_chunk.is_none() {
            continue;
        }
        let chunk = maybe_chunk.unwrap();
        let (min, max) = chunk_min_max(&chunk, metadata.opts.index_cols);

        let popts = parquet::write::WriteOptions {
            write_statistics: true,
            compression: parquet::write::CompressionOptions::Snappy,
            version: parquet::write::Version::V1,
            // todo define page size
            data_pagesize_limit: None,
        };

        let chunk_len = chunk.len();
        let encodings = metadata
            .schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| parquet::write::Encoding::Plain))
            .collect();
        let row_groups = parquet::write::RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &metadata.schema,
            popts,
            encodings,
        )?;

        let path = part_path(
            &path,
            metadata.table_name.as_str(),
            pid,
            0,
            metadata.partitions[pid].levels[0].part_id,
        );
        // !@#trace!("creating part file {:?}", p);
        let w = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut writer =
            parquet::write::FileWriter::try_new(BufWriter::new(w), metadata.schema.clone(), popts)?;
        // Write the part
        for group in row_groups {
            writer.write(group?)?;
        }
        let sz = writer.end(None)?;
        ret.push((pid, Part {
            id: metadata.partitions[pid].levels[0].part_id,
            size_bytes: sz,
            values: chunk_len,
            min,
            max,
        }))
    }
    // !@#trace!("{:?} bytes written", sz);
    Ok(ret)
}

fn flush(
    log: &mut File,
    memtable: &mut Memtable,
    metadata: &mut Metadata,
    path: &PathBuf,
) -> Result<File> {
    let start_time = Instant::now();

    // in case when it is flushed by another thread
    log.flush()?;
    log.sync_all()?;
    // swap memtable
    // !@#trace!("swapping memtable");

    // write to parquet
    // !@#trace!("writing to parquet");
    // !@#trace!("part id: {}", md.levels[0].part_id);
    let empty_memtable = memtable.create_empty();
    let memtable = mem::replace(memtable, empty_memtable);
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
    log_metadata(&mut log, metadata);

    trace!(
        "removing previous log file {:?}",
        format!("{:016}.log", metadata.log_id - 1)
    );
    fs::remove_file(path.join(format!(
        "tables/{}/{:016}.log",
        metadata.table_name,
        metadata.log_id - 1
    )))?;

    histogram!("store.flush_time_seconds").record(start_time.elapsed());
    counter!("store.flushes_total").increment(1);

    Ok(log)
}

pub struct ScanStream {
    iter: Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>,
    start_time: Instant,
    table_name: String, // for metrics
}

impl ScanStream {
    pub fn new(
        iter: Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>,
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
                histogram!("store.scan_time_seconds","table"=>self.table_name.to_string())
                    .record(self.start_time.elapsed());
                Poll::Ready(None)
            }
            Some(chunk) => Poll::Ready(Some(chunk)),
        }
    }
}

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }

    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&self, tbl_name: &str, mut values: Vec<NamedValue>) -> Result<()> {
        let start_time = Instant::now();
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();

        drop(tables);

        let mut metadata = tbl.metadata.lock();
        for v in values.iter() {
            match metadata.schema.fields.iter().find(|f| f.name == v.name) {
                None => {
                    return Err(StoreError::Internal(format!(
                        "column {} not found in schema",
                        v.name
                    )));
                }
                f => match (&v.value, &f.unwrap().data_type) {
                    (Value::Int8(_), DataType::Int8) => {}
                    (Value::Int16(_), DataType::Int16) => {}
                    (Value::Int32(_), DataType::Int32) => {}
                    (Value::Int64(_), DataType::Int64) => {}
                    (Value::Boolean(_), DataType::Boolean) => {}
                    (Value::Decimal(_), DataType::Decimal(_, _)) => {}
                    (Value::String(_), DataType::Utf8) => {}
                    (Value::Timestamp(_), DataType::Timestamp(_, _)) => {}
                    _ => {
                        return Err(StoreError::Internal(format!(
                            "column {} ({:?}) has different type: {:?}",
                            v.name,
                            v.value,
                            f.unwrap().data_type
                        )));
                    }
                },
            }
        }

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

            final_values.push(Value::Null);
        }
        let mut log = tbl.log.lock();
        let logged = log_op(
            LogOp::Insert(pk_values.clone(), final_values.clone()),
            log.get_mut(),
        )?;
        metadata.stats.logged_bytes += logged as u64;
        metadata.seq_id += 1;

        let mut memtable = tbl.memtable.lock();

        let phash = siphash(&pk_values);
        let pid = phash as usize % metadata.opts.partitions;
        for (idx, val) in pk_values.iter().enumerate() {
            memtable.partitions[pid].push_value(idx, val.into());
        }
        for (idx, val) in final_values.into_iter().enumerate() {
            memtable.partitions[pid].push_value(idx + metadata.opts.index_cols, val);
        }

        if metadata.stats.logged_bytes as usize > metadata.opts.max_log_length_bytes {
            let l = flush(log.get_mut(), &mut memtable, &mut metadata, &self.path)?;
            *log = BufWriter::new(l);

            // if md.levels[0].parts.len() > 0 && md.levels[0].parts.len() > self.opts.l0_max_parts {
            //     self.compactor_outbox
            //         .send(CompactorMessage::Compact)
            //         .unwrap();
            // }
        }
        counter!("store.inserts_total", "table"=>tbl_name.to_string()).increment(1);
        histogram!("store.insert_time_seconds","table"=>tbl_name.to_string())
            .record(start_time.elapsed());
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
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let mut tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };

        drop(tables);
        // locking compactor file operations (deleting/moving) to prevent parts deletion during scanning
        let _vfs = tbl.vfs.lock.lock();
        let md = tbl.metadata.lock();
        let metadata = md.clone();
        drop(md);
        let partition = metadata
            .partitions
            .iter()
            .find(|p| p.id == partition_id)
            .unwrap()
            .to_owned();
        let start_time = Instant::now();
        let memtable = tbl.memtable.lock();

        let mut fields_idx = vec![];
        for (idx, f) in metadata.schema.fields.iter().enumerate() {
            for ff in &fields {
                if &f.name == ff {
                    fields_idx.push(idx);
                }
            }
        }
        let maybe_chunk =
            memtable.partitions[partition_id].chunk(Some(fields_idx), metadata.opts.index_cols)?;
        histogram!("store.scan_memtable_seconds","table"=>tbl_name.to_string())
            .record(start_time.elapsed());
        let mut rdrs = vec![];
        // todo remove?

        for (level_id, level) in partition.levels.into_iter().enumerate() {
            for part in level.parts {
                let path = part_path(&self.path, tbl_name, partition.id, level_id, part.id);
                let rdr = BufReader::new(File::open(path)?);
                rdrs.push(rdr);
            }
        }
        counter!("store.scan_parts_total", "table"=>tbl_name.to_string())
            .increment(rdrs.len() as u64);
        let iter = if rdrs.len() == 0 {
            Box::new(MemChunkIterator::new(maybe_chunk))
                as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>
        } else {
            let opts = arrow_merger::Options {
                index_cols: metadata.opts.merge_index_cols,
                array_size: metadata.opts.merge_array_size,
                chunk_size: metadata.opts.merge_chunk_size,
                fields: fields.clone(),
            };
            // todo fix
            Box::new(MergingIterator::new(
                rdrs,
                maybe_chunk,
                metadata.schema.clone(),
                opts,
            )?) as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>
        };

        counter!("store.scans_total", "table"=>tbl_name.to_string()).increment(1);
        Ok(ScanStream::new(iter, tbl_name.to_string()))
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn add_field(
        &self,
        tbl_name: &str,
        field_name: &str,
        dt: DType,
        is_nullable: bool,
    ) -> Result<()> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();
        drop(tables);

        let mut metadata = tbl.metadata.lock();
        for f in &metadata.schema.fields {
            if f.name == field_name {
                return Err(StoreError::AlreadyExists(format!(
                    "Field with name {} already exists",
                    field_name
                )));
            }
        }

        let field = Field::new(field_name.to_string(), dt.clone().try_into()?, is_nullable);
        metadata.schema.fields.push(field.clone());
        let mut log = tbl.log.lock();
        log_op(LogOp::AddField(field.clone()), log.get_mut())?;

        let mut memtable = tbl.memtable.lock();
        if memtable.cols_len() < metadata.schema.fields.len() {
            let new_cols = metadata.schema.fields.len() - memtable.cols_len();
            drop(metadata);
            for _ in 0..new_cols {
                memtable.add_column(DType::try_from(field.data_type.clone())?);
            }
        }
        Ok(())
    }

    pub fn create_table(&self, table_name: String, opts: TableOptions) -> Result<()> {
        let tbl = self.tables.read();
        if tbl.iter().find(|t| t.name == table_name).is_some() {
            return Err(StoreError::AlreadyExists(format!(
                "Table with name {} already exists",
                table_name
            )));
        }
        drop(tbl);
        let path = self.path.join("tables").join(table_name.clone());
        fs::create_dir_all(&path)?;
        for pid in 0..opts.partitions {
            for lid in 0..opts.levels {
                fs::create_dir_all(path.join(pid.to_string()).join(lid.to_string()))?;
            }
        }

        let mut metadata = Metadata {
            version: 0,
            seq_id: 0,
            log_id: 0,
            table_name: table_name.clone(),
            schema: Schema::from(vec![]),
            stats: Default::default(),
            partitions: (0..opts.partitions)
                .into_iter()
                .map(|pid| Partition {
                    id: pid,
                    levels: vec![Level::new_empty(); opts.levels],
                })
                .collect::<Vec<_>>(),
            opts: opts.clone(),
        };

        let memtable = Memtable::new(opts.partitions.clone());
        let mut log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.join(format!("{:016}.log", 0)))?;
        log_metadata(&mut log, &mut metadata)?;
        let tbl = Table {
            name: table_name.clone(),
            memtable: Arc::new(Mutex::new(memtable)),
            metadata: Arc::new(Mutex::new(metadata)),
            vfs: Arc::new(Vfs::new()),
            log: Arc::new(Mutex::new(BufWriter::new(log))),
        };

        let mut tables = self.tables.write();
        tables.push(tbl);
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
            println!("[ERROR] failed to shut down compaction worker on store drop");
            return;
        }
        for _ in rx {}
        println!("[INFO] store successfully stopped");
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
    use bincode::serialize;
    use common::types::DType;
    use futures::executor::block_on;
    use futures::Stream;
    use futures::StreamExt;
    use get_size::GetSize;
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
    fn gen() {
        // let path = temp_dir().join("db");
        let path = PathBuf::from("/opt/homebrew/Caskroom/clickhouse/user_files");
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir_all(&path).unwrap();

        let opts = Options {};
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        let topts = TableOptions {
            levels: 7,
            merge_array_size: 1000,
            partitions: 2,
            index_cols: 2,
            l1_max_size_bytes: 1024,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024,
            merge_array_page_size: 10000,
            merge_data_page_size_limit_bytes: Some(1024 * 1024),
            merge_index_cols: 2,
            merge_max_l1_part_size_bytes: 1024 * 1024,
            merge_part_size_multiplier: 10,
            merge_row_group_values_limit: 1000,
            merge_max_page_size: 100,
            merge_chunk_size: 1024 * 8 * 8,
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

        let cols = 4;
        db.create_table("t1".to_string(), topts).unwrap();
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
                    .map(|v| NamedValue::new(v.to_string(), Value::Int64(Some(i as i64))))
                    .collect::<Vec<_>>();
                db.insert("t1", vals).unwrap();
            }
        }
        // let names = (0..cols).map(|v| v.to_string()).collect::<Vec<_>>();
        // let streams = db.scan_partition("t1", 2, names).unwrap();
        // for stream in streams {
        // let b = block_on(stream.collect::<Vec<_>>())
        // .into_iter()
        // .map(|v| v.unwrap())
        // .collect::<Vec<_>>();
        //
        // for bb in b {
        //     println!("{: ?}", bb);
        // }
        // }

        db.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        // print_partitions(db.tables.read()[0].metadata.lock().partitions.as_ref());
        // db.compact();
    }

    #[test]
    fn test_schema_evolution() {
        let path = PathBuf::from("/opt/homebrew/Caskroom/clickhouse/user_files");
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
            merge_index_cols: 1,
            merge_max_l1_part_size_bytes: 1024 * 1024,
            merge_part_size_multiplier: 10,
            merge_row_group_values_limit: 1000,
            merge_chunk_size: 1024 * 8 * 8,
            merge_max_page_size: 1024 * 1024,
        };

        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();

        db.insert("t1", vec![NamedValue::new(
            "f1".to_string(),
            Value::Int64(Some(1)),
        )])
        .unwrap();

        db.add_field("t1", "f2", DType::Int64, false).unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();

        db.flush().unwrap();
    }
}
