use std::collections::BinaryHeap;
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::mem;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use arrow2::array::Array;
use arrow2::array::BooleanArray;
use arrow2::array::Int128Array;
use arrow2::array::Int16Array;
use arrow2::array::Int32Array;
use arrow2::array::Int64Array;
use arrow2::array::Int8Array;
use arrow2::array::ListArray;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::io::parquet;
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::transverse;
use arrow_array::StringArray;
use arrow_array::TimestampNanosecondArray;
use bincode::deserialize;
use bincode::serialize;
use common::types::DType;
use futures::Stream;
use lru::LruCache;
use metrics::counter;
use metrics::gauge;
use metrics::histogram;
use num_traits::ToPrimitive;
use parking_lot::Mutex;
use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip::SipHasher13;
use tracing::trace;

use crate::arrow_conversion::schema2_to_schema1;
use crate::compaction::Compactor;
use crate::compaction::CompactorMessage;
use crate::error::Result;
use crate::error::StoreError;
use crate::memtable::Memtable;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::parquet::arrow_merger;
use crate::parquet::arrow_merger::MemChunkIterator;
use crate::parquet::arrow_merger::MergingIterator;
use crate::parquet::chunk_min_max;
use crate::table;
use crate::table::part_path;
use crate::table::Level;
use crate::table::Metadata;
use crate::table::Part;
use crate::table::Table;
use crate::Fs;
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
            histogram!("store.memtable_rows","table"=>t.name.to_string())
                .record(mem_mx.len().to_f64().unwrap());
            drop(mem_mx);

            gauge!("store.table_fields", "table"=>t.name.to_string())
                .set(md.schema.fields.len().to_f64().unwrap());
            gauge!("store.sequence","table"=>t.name.to_string()).set(md.seq_id.to_f64().unwrap());
            for (lvlid, lvl) in md.levels.iter().enumerate() {
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
                gauge!("store.parts_values","table"=>t.name.to_string(),"level"=>lvlid.to_string())
                    .set(values.to_f64().unwrap());
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum LogOp {
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
            let _pid = phash as usize % metadata.opts.parallelism;
            let kv: Vec<Value> = k.iter().map(|k| k.into()).collect::<Vec<_>>();
            let vv = [kv, v].concat();
            for (idx, val) in vv.into_iter().enumerate() {
                memtable.push_value(idx, val);
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

pub(crate) fn log_metadata(log: &mut File, metadata: &mut Metadata) -> Result<()> {
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

    if logs.is_empty() {
        return Err(StoreError::Internal("no logs found".to_string()));
    }

    // !@#trace!("found {} logs", logs.len());

    let log_path = logs.pop().unwrap();
    // !@#trace!("last log: {:?}", log_path);
    // todo make a buffered read. Currently something odd happens with reading the length of the record
    let mut log = OpenOptions::new().read(true).write(true).open(&log_path)?;
    // get metadata (first record of the log)

    let mut crc_b = [0u8; mem::size_of::<u32>()];
    let read_bytes = log.read(&mut crc_b)?;
    if read_bytes == 0 {
        return Err(StoreError::Internal("empty log file".to_string()));
    }
    let crc32 = u32::from_le_bytes(crc_b);
    // !@#trace!("crc32: {}", crc32);

    let mut len_b = [0u8; mem::size_of::<u64>()];
    _ = log.read(&mut len_b)?;
    let len = u64::from_le_bytes(len_b);
    // !@#trace!("len: {}", len);
    let mut data_b = vec![0u8; len as usize];
    _ = log.read(&mut data_b)?;
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

    let mut memtable = Memtable::new();

    for f in &metadata.schema.fields {
        memtable.add_column(f.data_type.clone().try_into()?);
    }
    loop {
        let mut crc_b = [0u8; mem::size_of::<u32>()];
        let read_bytes = log.read(&mut crc_b)?;
        if read_bytes == 0 {
            break;
        }
        let crc32 = u32::from_le_bytes(crc_b);
        // !@#trace!("crc32: {}", crc32);

        let mut len_b = [0u8; mem::size_of::<u64>()];
        _ = log.read(&mut len_b)?;
        let len = u64::from_le_bytes(len_b);
        // !@#trace!("len: {}", len);
        let mut data_b = vec![0u8; len as usize];
        _ = log.read(&mut data_b)?;
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
        vfs: Arc::new(Fs::new()),
        log: Arc::new(Mutex::new(log)),
        cas: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
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
    let tables_cloned = tables.clone();
    thread::spawn(move || collect_metrics(tables_cloned));
    Ok(OptiDBImpl {
        opts,
        tables,
        compactor_outbox,
        path,
    })
}

fn write_level0(metadata: &Metadata, memtable: &Memtable, path: PathBuf) -> Result<Part> {
    let chunk = memtable
        .chunk(None, metadata.opts.index_cols, metadata.opts.is_replacing)?
        .unwrap();
    let (min, max) = chunk_min_max(&chunk, metadata.opts.index_cols);
    let popts = parquet::write::WriteOptions {
        write_statistics: true,
        compression: parquet::write::CompressionOptions::Snappy,
        version: parquet::write::Version::V2,
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
        0,
        metadata.levels[0].part_id,
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
    let part = Part {
        id: metadata.levels[0].part_id,
        size_bytes: sz,
        values: chunk_len,
        min,
        max,
    };

    Ok(part)
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

    let part = write_level0(metadata, &memtable, path.to_owned())?;
    metadata.levels[0].parts.push(part.clone());
    // increment table id
    metadata.levels[0].part_id += 1;
    metadata.stats.written_bytes += part.size_bytes;

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

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.next() {
            None => {
                histogram!("store.scan_time_seconds","table"=>self.table_name.to_string())
                    .record(self.start_time.elapsed());
                Poll::Ready(None)
            }
            Some(chunk) => {
                let chunk = chunk?;
                Poll::Ready(Some(Ok(chunk)))
            }
        }
    }
}

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }

    pub fn scan(&self, tbl_name: &str, projection: Vec<usize>) -> Result<ScanStream> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };

        drop(tables);
        // locking compactor file operations (deleting/moving) to prevent parts deletion during scanning
        let _vfs = tbl.vfs.lock.lock();
        let md = tbl.metadata.lock();
        let metadata = md.clone();
        drop(md);

        let start_time = Instant::now();
        let memtable = tbl.memtable.lock();

        let projected_fields = metadata
            .schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, f)| {
                if projection.contains(&idx) {
                    Some(f.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let maybe_chunk = memtable.chunk(
            Some(projection),
            metadata.opts.index_cols,
            metadata.opts.is_replacing,
        )?;
        histogram!("store.scan_memtable_seconds","table"=>tbl_name.to_string())
            .record(start_time.elapsed());
        let mut rdrs = vec![];
        // todo remove?

        for (level_id, level) in metadata.levels.into_iter().enumerate() {
            for part in level.parts {
                let path = part_path(&self.path, tbl_name, level_id, part.id);
                let rdr = BufReader::new(File::open(path)?);
                rdrs.push(rdr);
            }
        }
        counter!("store.scan_parts_total", "table"=>tbl_name.to_string())
            .increment(rdrs.len() as u64);
        let iter = if rdrs.is_empty() {
            Box::new(MemChunkIterator::new(maybe_chunk))
                as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>
        } else {
            let opts = arrow_merger::Options {
                index_cols: metadata.opts.index_cols,
                is_replacing: metadata.opts.is_replacing,
                array_size: metadata.opts.merge_array_size,
                chunk_size: metadata.opts.merge_chunk_size,
                fields: projected_fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect::<Vec<_>>(),
            };
            // todo fix
            Box::new(MergingIterator::new(
                rdrs,
                maybe_chunk,
                Schema::from(projected_fields),
                opts,
            )?) as Box<dyn Iterator<Item = Result<Chunk<Box<dyn Array>>>> + Send>
        };

        counter!("store.scans_total", "table"=>tbl_name.to_string()).increment(1);
        Ok(ScanStream::new(iter, tbl_name.to_string()))
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

        for field in metadata.schema.fields.iter().skip(metadata.opts.index_cols) {
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

        for (idx, val) in pk_values.iter().enumerate() {
            memtable.push_value(idx, val.into());
        }
        for (idx, val) in final_values.into_iter().enumerate() {
            memtable.push_value(idx + metadata.opts.index_cols, val);
        }

        if memtable.len() > 0
            && metadata.stats.logged_bytes as usize > metadata.opts.max_log_length_bytes
        {
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

    pub fn get_cas(&self, tbl_name: &str, key: Vec<KeyValue>) -> Result<Option<Vec<Value>>> {
        let res = self.get(tbl_name, key.clone())?;
        if res.is_none() {
            return Ok(None);
        }

        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };
        drop(tables);
        let md = tbl.metadata.lock();
        let metadata = md.clone();
        drop(md);

        if let Some(res) = &res {
            if let Value::Int64(Some(cas)) = res[metadata.opts.index_cols - 1] {
                let mut cache = tbl.cas.write();
                let key = res[..metadata.opts.index_cols - 1].to_vec();

                cache.put(key, cas);
            } else {
                unreachable!();
            }
        }

        Ok(res)
    }

    pub fn replace(&self, tbl_name: &str, values: Vec<NamedValue>) -> Result<()> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };
        drop(tables);
        let md = tbl.metadata.lock();
        let metadata = md.clone();
        drop(md);

        if let Value::Int64(Some(cas)) = values[metadata.opts.index_cols - 1].value {
            let mut cache = tbl.cas.write();
            let key = values[..metadata.opts.index_cols - 1]
                .iter()
                .map(|nv| nv.value.to_owned())
                .collect::<Vec<_>>();
            if let Some(c) = cache.pop(&key) {
                if c != cas {
                    return Err(StoreError::Internal("cas mismatch".to_string()));
                }
            }
        } else {
            unreachable!();
        }

        self.insert(tbl_name, values)
    }

    pub fn get(&self, tbl_name: &str, key: Vec<KeyValue>) -> Result<Option<Vec<Value>>> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name);
        let tbl = match tbl {
            None => return Err(StoreError::Internal("table not found".to_string())),
            Some(tbl) => tbl.to_owned(),
        };
        drop(tables);
        let mt = tbl.memtable.lock();
        if let Some(v) = mt.get(&key) {
            return Ok(Some(v));
        }
        drop(mt);
        let md = tbl.metadata.lock();
        let metadata = md.clone();
        drop(md);

        for (level_id, level) in metadata.levels.iter().enumerate() {
            let mut parts = level.parts.clone();
            parts.reverse();
            for part in parts.iter() {
                let offset = if metadata.opts.is_replacing { 1 } else { 0 };
                if key >= part.min[..part.min.len() - offset].to_vec()
                    && key <= part.max[..part.max.len() - offset].to_vec()
                {
                    let path = part_path(&self.path, tbl_name, level_id, part.id);
                    let mut rdr = File::open(path)?;
                    let parquet_md = read::read_metadata(&mut rdr)?;
                    let schema = read::infer_schema(&parquet_md)?;

                    let mut found = None;
                    'g: for row_group_id in 0..parquet_md.row_groups.len() {
                        for field_id in 0..key.len() {
                            let field = &schema.fields[field_id];
                            let statistics = read::statistics::deserialize(field, &[parquet_md
                                .row_groups[row_group_id]
                                .clone()])?;
                            // todo support non-int64 values
                            let min = statistics
                                .min_value
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            // assume we have only one value
                            let minv = min.value(0);

                            let max = statistics
                                .max_value
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap();
                            // assume we have only one value
                            let maxv = max.value(0);

                            if let KeyValue::Int64(v) = key[field_id] {
                                if v < minv || v > maxv {
                                    continue 'g;
                                }
                            } else {
                                unreachable!();
                            }
                        }

                        found = Some(row_group_id);
                        break;
                    }

                    if found.is_none() {
                        return Ok(None);
                    }

                    let mut chunks = read::FileReader::new(
                        rdr,
                        vec![parquet_md.row_groups[found.unwrap()].clone()],
                        schema.clone(),
                        Some(1024 * 8 * 8), // todo make configurable?
                        None,
                        None,
                    );

                    let int_v = key
                        .iter()
                        .map(|v| match v {
                            KeyValue::Int64(v) => *v,
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>();

                    for chunk in chunks.next() {
                        let chunk = chunk?;

                        let mut idx_cols = vec![];
                        for field_id in 0..key.len() {
                            idx_cols.push(
                                chunk.arrays()[field_id]
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap(),
                            );
                        }

                        's: for row_id in 0..chunk.len() {
                            let v = idx_cols[0].value(row_id);
                            if int_v[0] != v {
                                continue;
                            }

                            for other in 1..key.len() {
                                let v = idx_cols[other].value(row_id);
                                if int_v[other] != v {
                                    continue 's;
                                }
                            }

                            let mut vals = vec![];

                            for (field_id, field) in metadata.schema.fields.iter().enumerate() {
                                if field_id > schema.fields.len() - 1 {
                                    let val = match &field.data_type {
                                        DataType::Boolean => Value::Null,
                                        DataType::Int8 => Value::Null,
                                        DataType::Int16 => Value::Null,
                                        DataType::Int32 => Value::Null,
                                        DataType::Int64 => Value::Null,
                                        DataType::Timestamp(_, _) => Value::Null,
                                        DataType::Decimal(_, _) => Value::Null,
                                        DataType::Utf8 => Value::Null,
                                        DataType::List(f) => match f.data_type {
                                            DataType::Boolean => Value::Null,
                                            DataType::Int8 => Value::Null,
                                            DataType::Int16 => Value::Null,
                                            DataType::Int32 => Value::Null,
                                            DataType::Int64 => Value::Null,
                                            DataType::Timestamp(_, _) => Value::Null,
                                            DataType::Decimal(_, _) => Value::Null,
                                            DataType::Utf8 => Value::Null,
                                            _ => unreachable!(),
                                        },
                                        _ => unreachable!(),
                                    };

                                    vals.push(val);
                                } else {
                                    if chunk.arrays()[field_id].is_null(row_id) {
                                        vals.push(Value::Null);
                                        continue;
                                    }

                                    match &field.data_type {
                                        DataType::Boolean => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<BooleanArray>()
                                                .unwrap();
                                            vals.push(Value::Boolean(Some(arr.value(row_id))));
                                        }
                                        DataType::Int8 => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<Int8Array>()
                                                .unwrap();
                                            vals.push(Value::Int8(Some(arr.value(row_id))));
                                        }
                                        DataType::Int16 => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<Int16Array>()
                                                .unwrap();
                                            vals.push(Value::Int16(Some(arr.value(row_id))));
                                        }
                                        DataType::Int32 => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<Int32Array>()
                                                .unwrap();
                                            vals.push(Value::Int32(Some(arr.value(row_id))));
                                        }
                                        DataType::Int64 => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<Int64Array>()
                                                .unwrap();
                                            vals.push(Value::Int64(Some(arr.value(row_id))));
                                        }
                                        DataType::Decimal(_, _) => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<Int128Array>()
                                                .unwrap();
                                            vals.push(Value::Decimal(Some(arr.value(row_id))));
                                        }
                                        DataType::Timestamp(_, _) => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<TimestampNanosecondArray>()
                                                .unwrap();
                                            vals.push(Value::Timestamp(Some(arr.value(row_id))));
                                        }
                                        DataType::Utf8 => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<StringArray>()
                                                .unwrap();
                                            vals.push(Value::String(Some(
                                                arr.value(row_id).to_string(),
                                            )));
                                        }
                                        DataType::List(f) => {
                                            let arr = chunk.arrays()[field_id]
                                                .as_any()
                                                .downcast_ref::<ListArray<i32>>()
                                                .unwrap();

                                            match f.data_type {
                                                DataType::Boolean => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<BooleanArray>()
                                                        .unwrap();
                                                    vals.push(Value::Boolean(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Int8 => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<Int8Array>()
                                                        .unwrap();
                                                    vals.push(Value::Int8(Some(arr.value(row_id))));
                                                }
                                                DataType::Int16 => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<Int16Array>()
                                                        .unwrap();
                                                    vals.push(Value::Int16(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Int32 => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<Int32Array>()
                                                        .unwrap();
                                                    vals.push(Value::Int32(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Int64 => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<Int64Array>()
                                                        .unwrap();
                                                    vals.push(Value::Int64(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Decimal(_, _) => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<Int128Array>()
                                                        .unwrap();
                                                    vals.push(Value::Decimal(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Timestamp(_, _) => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<TimestampNanosecondArray>()
                                                        .unwrap();
                                                    vals.push(Value::Timestamp(Some(
                                                        arr.value(row_id),
                                                    )));
                                                }
                                                DataType::Utf8 => {
                                                    let arr = arr
                                                        .as_any()
                                                        .downcast_ref::<StringArray>()
                                                        .unwrap();
                                                    vals.push(Value::String(Some(
                                                        arr.value(row_id).to_string(),
                                                    )));
                                                }
                                                _ => unreachable!(),
                                            }
                                        }
                                        _ => unreachable!(),
                                    }
                                }
                            }

                            return Ok(Some(vals));
                        }
                    }
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
    pub fn parts_path(&self, tbl_name: &str) -> Result<Vec<String>> {
        let tables = self.tables.read();
        let tbl = tables.iter().find(|t| t.name == tbl_name).cloned().unwrap();
        drop(tables);

        let mut ret = vec![];
        let md = tbl.metadata.lock();
        for (level_id, level) in md.levels.iter().enumerate() {
            for part in &level.parts {
                let path = part_path(&self.path, tbl_name, level_id, part.id);
                ret.push(path.into_os_string().into_string().unwrap());
            }
        }

        Ok(ret)
    }
    pub fn compact(&self) {
        self.compactor_outbox
            .send(CompactorMessage::Compact)
            .unwrap();
    }

    pub fn flush(&self) -> Result<()> {
        let tbls = self.tables.read();
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
        let tbl = match tbl {
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

    pub fn delete(&self, _opts: WriteOptions, _key: Vec<KeyValue>) -> Result<()> {
        unimplemented!()
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

    pub fn create_table(&self, table_name: String, opts: table::Options) -> Result<()> {
        let tbl = self.tables.read();
        if tbl.iter().any(|t| t.name == table_name) {
            return Err(StoreError::AlreadyExists(format!(
                "Table with name {} already exists",
                table_name
            )));
        }
        drop(tbl);
        let path = self.path.join("tables").join(table_name.clone());
        fs::create_dir_all(&path)?;
        for lid in 0..opts.levels {
            fs::create_dir_all(path.join(lid.to_string()))?;
        }

        let mut metadata = Metadata {
            version: 0,
            seq_id: 0,
            log_id: 0,
            table_name: table_name.clone(),
            schema: Schema::from(vec![]),
            stats: Default::default(),
            levels: vec![Level::new_empty(); opts.levels],
            opts: opts.clone(),
        };

        let memtable = Memtable::new();
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
            vfs: Arc::new(Fs::new()),
            log: Arc::new(Mutex::new(BufWriter::new(log))),
            cas: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(10).unwrap()))),
        };

        let mut tables = self.tables.write();
        tables.push(tbl);
        Ok(())
    }

    pub fn table_options(&self, tbl_name: &str) -> Result<table::Options> {
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

    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow2::array::Array;
    use arrow2::array::Int64Array;
    use arrow2::chunk::Chunk;
    use common::types::DType;

    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::table;
    use crate::KeyValue;
    use crate::NamedValue;
    use crate::Value;

    #[test]
    fn test_cas() {
        let path = PathBuf::from("/tmp/cas");

        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();
        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: false,
        };
        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        db.add_field("t1", "f3", DType::Int64, false).unwrap();
        db.replace("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(1))),
        ])
        .unwrap();

        let res = db
            .get_cas("t1", vec![KeyValue::Int64(1), KeyValue::Int64(1)])
            .unwrap();

        db.replace("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();

        let res = db
            .get_cas("t1", vec![KeyValue::Int64(1), KeyValue::Int64(2)])
            .unwrap();

        db.replace("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(3))),
        ])
        .unwrap();

        let res = db
            .get_cas("t1", vec![KeyValue::Int64(1), KeyValue::Int64(2)])
            .unwrap();

        let r = db.replace("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(3))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(3))),
        ]);

        assert!(r.is_err());
    }
    #[test]
    fn test_get() {
        let path = PathBuf::from("/tmp/get");
        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();
        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: true,
        };
        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        db.add_field("t1", "f3", DType::Int64, false).unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(1))),
        ])
        .unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(3))),
        ])
        .unwrap();

        // get from memtable
        let res = db.get_cas("t1", vec![KeyValue::Int64(1)]).unwrap();

        assert_eq!(
            res,
            Some(vec![
                Value::Int64(Some(1)),
                Value::Int64(Some(2)),
                Value::Int64(Some(2))
            ])
        );
    }

    #[test]
    fn test_get_full() {
        let path = PathBuf::from("/tmp/get_full");
        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();
        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: true,
        };
        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        let mut f = 3;
        for i in 0..2000 {
            db.insert("t1", vec![
                NamedValue::new("f1".to_string(), Value::Int64(Some(i))),
                NamedValue::new("f2".to_string(), Value::Int64(Some(i))),
            ])
            .unwrap();

            if i % 150 == 0 {
                db.flush().unwrap();
                db.compact();
                db.add_field("t1", format!("f{f}").as_str(), DType::Int64, true)
                    .unwrap();
                f += 1;
            }
        }

        let res = db
            .get("t1", vec![KeyValue::Int64(1), KeyValue::Int64(1)])
            .unwrap();

        dbg!(res);
    }

    #[test]
    fn test_update() {
        let path = PathBuf::from("/tmp/update");
        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();
        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: true,
        };
        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        db.add_field("t1", "f3", DType::Int64, false).unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))), // pk
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))), // version
            NamedValue::new("f3".to_string(), Value::Int64(Some(1))), // counter
        ])
        .unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(3))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(3))),
        ])
        .unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(1))),
        ])
        .unwrap();

        let r = db.get("t1", vec![KeyValue::Int64(1)]).unwrap();
        assert!(r.is_some());
        let r = db
            .get("t1", vec![KeyValue::Int64(1), KeyValue::Int64(1)])
            .unwrap();
        assert!(r.is_some());
        let r = db
            .get("t1", vec![KeyValue::Int64(1), KeyValue::Int64(3)])
            .unwrap();
        assert!(r.is_some());

        let r = db
            .get("t1", vec![KeyValue::Int64(1), KeyValue::Int64(4)])
            .unwrap();
        assert!(r.is_none());

        db.flush().unwrap();

        let r = db.get("t1", vec![KeyValue::Int64(1)]).unwrap();
        assert_eq!(
            r,
            Some(vec![
                Value::Int64(Some(1)),
                Value::Int64(Some(3)),
                Value::Int64(Some(3))
            ])
        );

        let r = db.get("t1", vec![KeyValue::Int64(2)]).unwrap();
        assert!(r.is_some());

        let r = db.get("t1", vec![KeyValue::Int64(3)]).unwrap();
        assert!(r.is_none());

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(4))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(4))),
        ])
        .unwrap();

        db.flush().unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(5))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(5))),
        ])
        .unwrap();
        db.flush().unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();

        db.flush().unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(3))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(3))),
        ])
        .unwrap();

        db.flush().unwrap();
        db.compact();

        let r = db.get("t1", vec![KeyValue::Int64(1)]).unwrap();
        assert_eq!(
            r,
            Some(vec![
                Value::Int64(Some(1)),
                Value::Int64(Some(5)),
                Value::Int64(Some(5))
            ])
        );
    }

    #[test]
    fn test_scan() {
        let path = PathBuf::from("/tmp/scan");
        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();
        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: true,
        };
        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))), // pk
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))), // version
        ])
        .unwrap();

        db.flush().unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))), // pk
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))), // version
        ])
        .unwrap();

        db.flush().unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))), // pk
            NamedValue::new("f2".to_string(), Value::Int64(Some(3))), // version
        ])
        .unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))), // pk
            NamedValue::new("f2".to_string(), Value::Int64(Some(4))), // version
        ])
        .unwrap();

        let mut stream = db.scan("t1", vec![0, 1]).unwrap();

        let exp = Chunk::new(vec![
            Box::new(Int64Array::from_vec(vec![1])) as Box<dyn Array>,
            Box::new(Int64Array::from_vec(vec![4])) as Box<dyn Array>,
        ]);
        let res = stream.iter.next().unwrap().unwrap();
    }
    #[test]
    fn test_schema_evolution() {
        let path = PathBuf::from("/tmp/schema_evolution");
        fs::remove_dir_all(&path).ok();
        fs::create_dir_all(&path).unwrap();

        let opts = Options {};
        let db = OptiDBImpl::open(path, opts).unwrap();
        let topts = table::Options {
            levels: 7,
            merge_array_size: 10000,
            parallelism: 1,
            index_cols: 2,
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
            is_replacing: false,
        };

        db.create_table("t1".to_string(), topts).unwrap();
        db.add_field("t1", "f1", DType::Int64, false).unwrap();
        db.add_field("t1", "f2", DType::Int64, false).unwrap();
        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
        ])
        .unwrap();
        db.add_field("t1", "f3", DType::Int64, false).unwrap();

        db.insert("t1", vec![
            NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
            NamedValue::new("f3".to_string(), Value::Int64(Some(2))),
        ])
        .unwrap();

        db.flush().unwrap();
    }
    // integration test is placed here and not in separate crate because conditional #[cfg(test)] is used
    // #[tokio::test]
    // async fn test_scenario() {
    // let path = PathBuf::from("/tmp/db_scenario");
    // fs::remove_dir_all(&path).ok();
    // fs::create_dir_all(&path).unwrap();
    //
    // let opts = db::Options {};
    // let db = OptiDBImpl::open(path.clone(), opts).unwrap();
    // let topts = table::Options {
    // levels: 7,
    // merge_array_size: 10000,
    // parallelism: 1,
    // index_cols: 1,
    // l1_max_size_bytes: 1024 * 1024 * 10,
    // level_size_multiplier: 10,
    // l0_max_parts: 1,
    // max_log_length_bytes: 1024 * 1024 * 100,
    // merge_array_page_size: 10000,
    // merge_data_page_size_limit_bytes: Some(1024 * 1024),
    // merge_index_cols: 1,
    // merge_max_l1_part_size_bytes: 1024 * 1024,
    // merge_part_size_multiplier: 10,
    // merge_row_group_values_limit: 1000,
    // merge_chunk_size: 1024 * 8 * 8,
    // merge_max_page_size: 1024 * 1024,
    // };
    //
    // db.create_table("t1".to_string(), topts).unwrap();
    // db.add_field("t1", "f1", DType::Int64, true).unwrap();
    //
    // db.insert("t1", vec![NamedValue::new(
    // "f1".to_string(),
    // Value::Int64(Some(1)),
    // )])
    // .unwrap();
    //
    // db.add_field("t1", "f2", DType::Int64, true).unwrap();
    //
    // db.insert("t1", vec![
    // NamedValue::new("f1".to_string(), Value::Int64(Some(2))),
    // NamedValue::new("f2".to_string(), Value::Int64(Some(2))),
    // ])
    // .unwrap();
    // scan two columns
    // let stream = db
    // .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(2)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // scan one column
    // let stream = db.scan_partition("t1", 0, vec!["f1".to_string()]).unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // new row
    // db.insert("t1", vec![
    // NamedValue::new("f1".to_string(), Value::Int64(Some(3))),
    // NamedValue::new("f2".to_string(), Value::Int64(None)),
    // ])
    // .unwrap();
    //
    // scan two columns
    // let stream = db
    // .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // let lpath = PathBuf::from("/tmp/db_scenario/tables/t1/0000000000000000.log");
    // assert!(fs::try_exists(lpath).unwrap());
    //
    // stop current instance
    // drop(db);
    //
    // reopen db
    // let opts = db::Options {};
    // let db = OptiDBImpl::open(path.clone(), opts).unwrap();
    //
    // scan two columns
    // let stream = db
    // .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // compact should compact anything
    // db.compact();
    //
    // flush to part
    // db.flush().unwrap();
    // let ppath = PathBuf::from("/tmp/db_scenario/tables/t1/0/0/0.parquet");
    // assert!(fs::try_exists(&ppath).unwrap());
    //
    // compact still shouldn't compact anything
    // db.compact();
    //
    // scan two columns
    // let stream = db
    // .scan_partition("t1", 0, vec!["f1".to_string(), "f2".to_string()])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // add one more row
    //
    // db.insert("t1", vec![
    // NamedValue::new("f1".to_string(), Value::Int64(Some(4))),
    // NamedValue::new("f2".to_string(), Value::Int64(Some(4))),
    // ])
    // .unwrap();
    //
    // db.flush().unwrap();
    //
    // add field
    // db.add_field("t1", "f3", DType::Int64, true).unwrap();
    //
    // insert into memtable
    // db.insert("t1", vec![
    // NamedValue::new("f1".to_string(), Value::Int64(Some(5))),
    // NamedValue::new("f2".to_string(), Value::Int64(Some(5))),
    // NamedValue::new("f3".to_string(), Value::Int64(Some(5))),
    // ])
    // .unwrap();
    //
    // scan should read from parquet as well as from memtable
    // let stream = db
    // .scan_partition("t1", 0, vec![
    // "f1".to_string(),
    // "f2".to_string(),
    // "f3".to_string(),
    // ])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(1), Some(2), Some(3)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(2), None]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None, None, None]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // let b = stream.next().await;
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(4)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![Some(4)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![None]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // let b = stream.next().await;
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
    // PrimitiveArray::<i64>::from(vec![Some(5)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // add values that intersect
    // db.insert("t1", vec![
    // NamedValue::new("f1".to_string(), Value::Int64(Some(1))),
    // NamedValue::new("f2".to_string(), Value::Int64(Some(1))),
    // NamedValue::new("f3".to_string(), Value::Int64(Some(1))),
    // ])
    // .unwrap();
    //
    // scan should read from parquet as well as from memtable
    // let stream = db
    // .scan_partition("t1", 0, vec![
    // "f1".to_string(),
    // "f2".to_string(),
    // "f3".to_string(),
    // ])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![
    // Some(1),
    // Some(1),
    // Some(2),
    // Some(3),
    // Some(4),
    // Some(5)
    // ])
    // .boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(1), Some(2), None, Some(4), Some(5)])
    // .boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(1), None, None, None, Some(5)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // compact
    // db.compact();
    // sleep because compaction is async
    // thread::sleep(Duration::from_millis(1));
    // let ppath = PathBuf::from("/tmp/db_scenario/tables/t1/0/1/1.parquet");
    // assert!(fs::try_exists(&ppath).unwrap());
    // scan once again
    //
    // scan should read from parquet as well as from memtable
    // let stream = db
    // .scan_partition("t1", 0, vec![
    // "f1".to_string(),
    // "f2".to_string(),
    // "f3".to_string(),
    // ])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![
    // Some(1),
    // Some(1),
    // Some(2),
    // Some(3),
    // Some(4),
    // Some(5)
    // ])
    // .boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(1), Some(2), None, Some(4), Some(5)])
    // .boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(1), None, None, None, Some(5)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    //
    // scan only 1 and 3 cols
    // let stream = db
    // .scan_partition("t1", 0, vec!["f1".to_string(), "f3".to_string()])
    // .unwrap();
    //
    // let mut stream: SendableRecordBatchStream = Box::pin(stream);
    // let b = stream.next().await;
    //
    // assert_eq!(
    // Chunk::new(vec![
    // PrimitiveArray::<i64>::from(vec![
    // Some(1),
    // Some(1),
    // Some(2),
    // Some(3),
    // Some(4),
    // Some(5)
    // ])
    // .boxed(),
    // PrimitiveArray::<i64>::from(vec![None, Some(1), None, None, None, Some(5)]).boxed(),
    // ]),
    // b.unwrap().unwrap()
    // );
    // }
}
