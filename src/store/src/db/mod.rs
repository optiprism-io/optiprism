use std::{cmp, io, time};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::mem;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::mpsc::RecvError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use rand::Rng;
use arrow2::array::Array;
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
use arrow2::io::parquet::write::transverse;
use arrow_buffer::ToByteSlice;
use bincode::deserialize;
use bincode::serialize;
use chrono::Duration;
use crossbeam_skiplist::SkipSet;
use futures::select;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use tracing::{debug, trace};
use tracing::error;
use tracing::instrument;
use tracing_subscriber::fmt::time::FormatTime;

use crate::error::Result;
use crate::error::StoreError;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::ColValue;
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
        $arr_ty::from(vals).as_arc()
    }};
}
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Part {
    id: usize,
    size_bytes: u64,
    min: Vec<KeyValue>,
    max: Vec<KeyValue>,
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
pub struct Metadata {
    version: u64,
    seq_id: u64,
    log_id: u64,
    schema: Schema,
    levels: Vec<Level>,
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
struct Options {
    l0_max_parts: usize,
    l1_max_size_bytes: usize,
    level_size_multiplier: usize,
    max_log_length_bytes: usize,
    merge_max_part_size_bytes: usize,
    merge_index_cols: usize,
    merge_data_page_size_limit_bytes: Option<usize>,
    merge_row_group_values_limit: usize,
    merge_array_page_size: usize,
}

enum CompactorMessage {
    Compact,
    Stop(mpsc::Sender<()>),
}

struct Compactor {
    opts: Options,
    metadata: Arc<Mutex<Metadata>>,
    log: Arc<Mutex<BufWriter<File>>>,
    path: PathBuf,
    inbox: Receiver<CompactorMessage>,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
struct ToCompact {
    level: usize,
    part: usize,
}

impl ToCompact {
    fn new(level: usize, part: usize) -> Self {
        ToCompact { level, part }
    }
}

fn determine_compaction(
    level_id: usize,
    level: &Level,
    next_level: &Level,
    opts: &Options,
) -> Result<Option<Vec<ToCompact>>> {
    println!("lid {} {}", level_id, level.parts.len());
    let mut to_compact = vec![];

    let level_parts = &level.parts;
    if level_id == 0 && level_parts.len() > opts.l0_max_parts {
        for part in level_parts {
            to_compact.push(ToCompact::new(0, part.id));
        }
    } else if level_id > 0 && level_parts.len() > 0 {
        let mut size = 0;
        for part in level_parts {
            size += part.size_bytes;
            let level_threshold =
                opts.l1_max_size_bytes * opts.level_size_multiplier.pow(level_id as u32 - 1);
            println!("threshold {} size {}", level_threshold, size);
            if size > level_threshold as u64 {
                to_compact.push(ToCompact::new(level_id, part.id));
            }
        }
    } else {
        return Ok(None);
    }
    if to_compact.is_empty() {
        return Ok(None);
    }
    let min = to_compact
        .iter()
        .map(|tc| level.get_part(tc.part).min)
        .min()
        .unwrap();
    let max = to_compact
        .iter()
        .map(|tc| level.get_part(tc.part).max)
        .max()
        .unwrap();
    for part in &next_level.parts {
        if cmp::max(part.min.clone(), min.clone()) < cmp::min(part.max.clone(), max.clone()) {
            to_compact.push(ToCompact::new(level_id + 1, part.id));
        }
    }

    Ok(Some(to_compact))
}

fn print_levels(levels: &[Level]) {
    for (idx, l) in levels.iter().enumerate() {
        println!("+-- {}", idx);
        for part in &l.parts {
            println!(
                "|   +-- id {} min {:?},max {:?}, size {}",
                part.id, part.min, part.max, part.size_bytes
            );
        }
    }
}

enum FsOp {
    Rename(PathBuf, PathBuf),
    Delete(PathBuf),
}

struct CompactResult {
    l0_remove: Vec<usize>,
    levels: Vec<Level>,
    fs_ops: Vec<FsOp>,
}

fn compact(levels: &[Level], path: &PathBuf, opts: &Options) -> Result<Option<CompactResult>> {
    let mut fs_ops = vec![];
    let mut l0_rem: Vec<(usize)> = Vec::new();
    let mut tmp_levels = levels.to_owned().clone();
    let mut compacted = false;
    for l in 0..tmp_levels.len() - 2 {
        let mut v = None;
        v = determine_compaction(l, &tmp_levels[l], &tmp_levels[l + 1], opts)?;
        match v {
            None => {
                if compacted {
                    break;
                } else {
                    return Ok(None);
                }
            }
            Some(to_compact) => {
                compacted = true;
                println!("to compact {:?}", to_compact);
                if to_compact.len() == 1 {
                    println!("rename");

                    let idx = tmp_levels[l].parts.iter().position(|p| p.id == to_compact[0].part).unwrap();

                    let mut part = tmp_levels[l].parts[idx].clone();
                    tmp_levels[l].parts.remove(idx);
                    part.id = tmp_levels[l + 1].part_id + 1;

                    let from = path.join(format!("parts/{}/{}.parquet", l, to_compact[0].part));
                    let to = path.join(format!("parts/{}/{}.parquet", l + 1, part.id));
                    fs_ops.push(FsOp::Rename(from, to));

                    tmp_levels[l + 1].parts.push(part.clone());
                    tmp_levels[l + 1].part_id += 1;
                    println!("done");
                    continue;
                }
                println!("llee {}", to_compact.len());
                let mut to_merge: Vec<Part> = vec![];
                for tc in &to_compact {
                    if tc.level == 0 {
                        l0_rem.push(tc.part);
                    }
                    let part = tmp_levels[tc.level]
                        .clone()
                        .parts
                        .into_iter()
                        .find(|p| p.id == tc.part)
                        .unwrap();
                    to_merge.push(part);
                    tmp_levels[tc.level].parts = tmp_levels[tc.level]
                        .clone()
                        .parts
                        .into_iter()
                        .filter(|p| p.id != tc.part)
                        .collect::<Vec<_>>();
                }

                let in_paths = to_merge.iter().map(|p| path.join(format!("parts/{}/{}.parquet", l, p.id))).collect::<Vec<_>>();
                println!("{:?}", in_paths);
                let out_part_id = tmp_levels[l + 1].part_id + 1;
                let out_path = path.join(format!("parts/{}", l + 1));
                let rdrs = in_paths.iter().map(|p| File::open(p)).collect::<std::result::Result<Vec<File>, std::io::Error>>()?;
                let merger_opts = merger::Options {
                    index_cols: opts.merge_index_cols,
                    data_page_size_limit_bytes: opts.merge_data_page_size_limit_bytes,
                    row_group_values_limit: opts.merge_row_group_values_limit,
                    array_page_size: opts.merge_array_page_size,
                    out_part_id,
                    max_part_size_bytes: Some(opts.merge_max_part_size_bytes),
                };
                let merge_result = merge(rdrs, out_path, out_part_id, merger_opts)?;
                println!("merge result {:#?}", merge_result);
                for f in merge_result {
                    let final_part = {
                        Part {
                            id: tmp_levels[l + 1].part_id + 1,
                            size_bytes: f.size_bytes,
                            min: f.min.iter().map(|v| v.try_into()).collect::<Result<Vec<_>>>()?,
                            max: f.max.iter().map(|v| v.try_into()).collect::<Result<Vec<_>>>()?,
                        }
                    };
                    tmp_levels[l + 1].parts.push(final_part);
                    tmp_levels[l + 1].part_id += 1;
                }

                fs_ops.append(in_paths.iter().map(|p| FsOp::Delete(p.clone())).collect::<Vec<_>>().as_mut());
                println!("after compaction");
            }
        }
    }

    println!("return lvl");
    Ok(Some(CompactResult {
        l0_remove: l0_rem,
        levels: tmp_levels,
        fs_ops,
    }))
}

impl Compactor {
    fn new(
        opts: Options,
        metadata: Arc<Mutex<Metadata>>,
        log: Arc<Mutex<BufWriter<File>>>,
        path: PathBuf,
        inbox: Receiver<CompactorMessage>,
    ) -> Self {
        Compactor {
            opts,
            metadata,
            log,
            path,
            inbox,
        }
    }
    fn run(self) {
        loop {
            match self.inbox.try_recv() {
                Ok(v) => {
                    match v {
                        CompactorMessage::Stop(dropper) => {
                            drop(dropper);
                            break;
                        }
                        _ => unreachable!()
                    }
                    println!("Terminating.");
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    break;
                }
                Err(TryRecvError::Empty) => {}
            }
            thread::sleep(time::Duration::from_micros(20));
            // match self.inbox.recv() {
            // Ok(msg) => match msg {
            //     CompactorMessage::Compact => {
            debug!("compaction started");
            let levels = {
                let md = self.metadata.lock().unwrap();
                md.levels.clone()
            };
            println!("md lvlb {}", levels[0].part_id);
            print_levels(&levels);
            match compact(&levels, &self.path, &self.opts) {
                Ok(res) => match res {
                    None => continue,
                    Some(res) => {
                        let mut md = self.metadata.lock().unwrap();
                        println!("md lvl");
                        print_levels(&md.levels);
                        // print_levels(&md.levels);
                        for rem in &res.l0_remove {
                            md.levels[0].parts = md.levels[0]
                                .parts
                                .clone()
                                .into_iter()
                                .filter(|p| p.id != *rem)
                                .collect::<Vec<_>>();

                            }
                        println!("res rem: {:?}",res.l0_remove);
                        println!("res levels");
                        print_levels(&res.levels);
                        for (idx, l) in res.levels.iter().enumerate().skip(1) {
                            md.levels[idx] = l.clone();
                        }
                        let mut log = self.log.lock().unwrap();
                        log_metadata(Some(log.get_mut()), &mut md).unwrap();
                        println!("md lvl after");
                        print_levels(&md.levels);
                        for op in res.fs_ops {
                            match op {
                                FsOp::Rename(from, to) => {
                                    trace!("renaming");
                                    // todo handle error
                                    fs::rename(from, to).unwrap();
                                }
                                FsOp::Delete(path) => {
                                    trace!("deleting {:?}",path);
                                    fs::remove_file(path).unwrap();
                                }
                            }
                        }
                        println!("After");
                        // let mut md = self.metadata.lock().unwrap();
                        // println!("post md");
                        print_levels(&md.levels);
                    }
                }
                Err(err) => {
                    error!("compaction error: {:?}", err);

                    continue;
                }
            }
        }
        //     CompactorMessage::Stop(dropper) => {
        //         drop(dropper);
        //         break;
        //     }
        // },
        // Err(err) => {
        //     trace!("unexpected compactor error: {:?}", err);
        //     break;
        // }
        // }
    }
}

struct OptiDBImpl {
    opts: Options,
    memtable: SkipSet<MemOp>,
    metadata: Arc<Mutex<Metadata>>,
    log: Arc<Mutex<BufWriter<File>>>,
    compactor_outbox: Sender<CompactorMessage>,
    path: PathBuf,
}

fn _log(op: &LogOp, metadata: &mut Metadata, log: &mut File) -> Result<()> {
    let data = serialize(op)?;

    let crc = hash_crc32(&data);
    trace!("crc32: {}", crc);
    log.write_all(&crc.to_le_bytes())?;
    log.write_all(&(data.len() as u64).to_le_bytes())?;
    log.write_all(&data)?;

    let logged_size = 8 + 4 + data.len();
    trace!("logged size: {}", logged_size);
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
        trace!("recover op: {:?}", op);
    } else {
        trace!("log op: {:?}", op);
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
    trace!("next op id: {}", metadata.seq_id);

    Ok(())
}

fn log_metadata(log: Option<&mut File>, metadata: &mut Metadata) -> Result<()> {
    trace!("log metadata");
    let log = log.unwrap();
    _log(&LogOp::Metadata(metadata.clone()), metadata, log)?;
    metadata.seq_id += 1;
    trace!("next op id: {}", metadata.seq_id);

    Ok(())
}

// #[instrument(level = "trace")]
fn recover(path: PathBuf, opts: Options) -> Result<OptiDBImpl> {
    trace!("db dir: {:?}", path);
    for i in 0..7 {
        fs::create_dir_all(path.join("parts").join(i.to_string()))?;
    }
    trace!("starting recovery");
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

    trace!("found {} logs", logs.len());

    let mut metadata = Metadata {
        version: 0,
        seq_id: 0,
        log_id: 0,
        schema: Schema::from(vec![]),
        stats: Default::default(),
        levels: vec![Level::new_empty(); 7],
    };

    let mut memtable = SkipSet::new();
    let log = if let Some(log_path) = logs.pop() {
        trace!("last log: {:?}", log_path);
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
            trace!("crc32: {}", crc32);

            let mut len_b = [0u8; mem::size_of::<u64>()];
            f.read_exact(&mut len_b)?;
            let len = u64::from_le_bytes(len_b);
            trace!("len: {}", len);
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
        trace!("operations recovered: {}", ops);
        drop(f);

        OpenOptions::new().append(true).open(log_path)?
    } else {
        trace!("creating initial log {}", format!("{:016}.log", 0));

        OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.join(format!("{:016}.log", 0)))?
    };

    metadata.stats.logged_bytes = log.metadata().unwrap().len();
    let trigger_compact = if metadata.levels[0].parts.len() > 0
        && metadata.levels[0].parts.len() > opts.l0_max_parts
    {
        true
    } else {
        false
    };
    let (compactor_outbox, rx) = std::sync::mpsc::channel();
    let metadata = Arc::new(Mutex::new(metadata));
    let log = Arc::new(Mutex::new(BufWriter::new(log)));
    let compactor = Compactor::new(opts.clone(), metadata.clone(), log.clone(), path.clone(), rx);
    thread::spawn(move || compactor.run());
    if trigger_compact {
        // compactor_outbox.send(CompactorMessage::Compact).unwrap();
    }
    Ok(OptiDBImpl {
        opts,
        memtable,
        metadata,
        log,
        compactor_outbox,
        path,
    })
}

fn write_level0(
    memtable: &SkipSet<MemOp>,
    part_id: usize,
    schema: &Schema,
    path: PathBuf,
) -> Result<Part> {
    let mut cols: Vec<Vec<Value>> = vec![Vec::new(); schema.fields.len()];
    trace!("{} entries to write", memtable.len());
    let mut min: Option<Vec<KeyValue>> = None;
    let mut max: Option<Vec<KeyValue>> = None;
    for (idx, op) in memtable.iter().enumerate() {
        let (keys, vals) = match &op.op {
            Op::Insert(k, v) => (k, Some(v)),
            Op::Delete(k) => (k, None),
        };
        if idx == 0 {
            min = Some(keys.clone());
        }
        if idx == memtable.len() - 1 {
            max = Some(keys.clone());
        }
        let idx_offset = keys.len();
        for (idx, key) in keys.into_iter().enumerate() {
            cols[idx].push(key.into());
        }
        if let Some(v) = vals {
            for (idx, val) in v.into_iter().enumerate() {
                cols[idx_offset + idx].push(val.clone());
            }
        }
    }

    let arrs = cols
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
                MutableBinaryArray::<i32>::from(vals).as_arc()
            }
            DataType::Utf8 => {
                let vals = col
                    .into_iter()
                    .map(|v| match v {
                        Value::String(b) => b,
                        _ => unreachable!("{:?}", v),
                    })
                    .collect::<Vec<_>>();
                MutableUtf8Array::<i32>::from(vals).as_arc()
            }
            DataType::Decimal(_, _) => {
                memory_col_to_arrow!(col, Decimal, MutablePrimitiveArray)
            }
            DataType::List(_) => unimplemented!(),
            _ => unimplemented!(),
        })
        .collect::<Vec<_>>();

    let popts = parquet::write::WriteOptions {
        write_statistics: true,
        compression: parquet::write::CompressionOptions::Snappy,
        version: parquet::write::Version::V1,
        data_pagesize_limit: None,
    };

    let chunk = Chunk::try_new(arrs)?;

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| parquet::write::Encoding::Plain))
        .collect();

    let row_groups = parquet::write::RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        schema,
        popts,
        encodings,
    )?;

    let p = path.join("parts").join("0").join(format!("{}.parquet", part_id));
    trace!("creating part file {:?}", p);
    let w = OpenOptions::new().create_new(true).write(true).open(p)?;
    let mut writer = parquet::write::FileWriter::try_new(w, schema.clone(), popts)?;
    // Write the part
    for group in row_groups {
        writer.write(group?)?;
    }
    let sz = writer.end(None)?;
    trace!("{:?} bytes written", sz);
    Ok(Part {
        id: part_id,
        size_bytes: sz,
        min: min.unwrap(),
        max: max.unwrap(),
    })
}

fn flush(
    log: Option<&mut File>,
    memtable: &mut SkipSet<MemOp>,
    md: &mut Metadata,
    path: &PathBuf,
) -> Result<()> {
    trace!("flushing log file");
    let f = log.unwrap();
    f.flush()?;
    f.sync_all()?;
    // swap memtable
    trace!("swapping memtable");
    let mut memtable = std::mem::take(memtable);

    // write to parquet
    trace!("writing to parquet");
    trace!("part id: {}", md.levels[0].part_id);
    let part = write_level0(&memtable, md.levels[0].part_id, &md.schema, path.clone())?;
    md.levels[0].parts.push(part.clone());
    // increment table id
    md.levels[0].part_id += 1;
    md.stats.written_bytes += part.size_bytes;
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

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&mut self, key: Vec<KeyValue>, values: Vec<Value>) -> Result<()> {
        let mut md = self.metadata.lock().unwrap();
        if key.len() + values.len() != md.schema.fields.len() {
            return Err(StoreError::Internal(format!(
                "Fields mismatch. Key+Val len: {}, schema fields len: {}",
                key.len() + values.len(),
                md.schema.fields.len()
            )));
        }
        log_op(
            LogOp::Insert(key, values),
            false,
            Some(self.log.lock().unwrap().get_mut()),
            &mut self.memtable,
            &mut md,
        )?;
        if md.stats.logged_bytes as usize > self.opts.max_log_length_bytes {
            flush(
                Some(self.log.lock().unwrap().get_mut()),
                &mut self.memtable,
                &mut md,
                &self.path,
            )?;

            // if md.levels[0].parts.len() > 0 && md.levels[0].parts.len() > self.opts.l0_max_parts {
            //     self.compactor_outbox
            //         .send(CompactorMessage::Compact)
            //         .unwrap();
            // }
        }
        Ok(())
    }

    fn compact(&self) {
        self.compactor_outbox
            .send(CompactorMessage::Compact)
            .unwrap();
    }
    fn flush(&mut self) -> Result<()> {
        let mut md = self.metadata.lock().unwrap();
        flush(
            Some(self.log.lock().unwrap().get_mut()),
            &mut self.memtable,
            &mut md,
            &self.path,
        )?;

        Ok(())
    }
    fn get(&self, opts: ReadOptions, key: Vec<KeyValue>) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }

    fn delete(&self, opts: WriteOptions, key: Vec<KeyValue>) -> Result<()> {
        unimplemented!()
    }

    // #[instrument(level = "trace", skip(self))]
    fn add_field(&mut self, field: Field) -> Result<()> {
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
    use rand::Rng;
    use tracing::info;
    use tracing::log;
    use tracing::log::debug;
    use tracing::trace;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_test::traced_test;

    use crate::db::{compact, print_levels};
    use crate::db::determine_compaction;
    use crate::db::Insert;
    use crate::db::Level;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::db::Part;
    use crate::db::ToCompact;
    use crate::ColValue;
    use crate::KeyValue;
    use crate::Value;

    #[traced_test]
    #[test]
    fn it_works() {
        // let path = temp_dir().join("db");
        let path = PathBuf::from("/Users/maximbogdanov/user_files");
        fs::remove_dir_all(&path).unwrap();
        // fs::create_dir_all(&path).unwrap();

        let opts = Options {
            l1_max_size_bytes: 1024,
            level_size_multiplier: 10,
            l0_max_parts: 4,
            max_log_length_bytes: 1024,
            merge_array_page_size: 1000,
            merge_data_page_size_limit_bytes: Some(1024),
            merge_index_cols: 2,
            merge_max_part_size_bytes: 2048,
            merge_row_group_values_limit: 1000,
        };
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        db.add_field(Field::new("a", DataType::Int64, false));
        db.add_field(Field::new("b", DataType::Int64, false));
        db.add_field(Field::new("c", DataType::Int64, false));
        for i in 0..1000 {
            thread::sleep(Duration::from_millis(1));
            db.insert(
                vec![
                    KeyValue::Int64(i),
                    KeyValue::Int64(i),
                ],
                vec![Value::Int64(Some(i))],
            )
                .unwrap();
        }

        // db.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        print_levels(db.metadata.lock().unwrap().levels.as_ref());
        // db.compact();
    }
}