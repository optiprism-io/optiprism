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
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;

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
use crossbeam_skiplist::SkipSet;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::Deserialize;
use serde::Serialize;
use tracing::instrument;
use tracing::trace;
use tracing_subscriber::fmt::time::FormatTime;

use crate::error::Result;
use crate::error::StoreError;
use crate::options::ReadOptions;
use crate::options::WriteOptions;
use crate::ColValue;
use crate::KeyValue;
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
#[derive(Debug, Serialize, Deserialize, Clone)]
struct Part {
    id: u64,
    size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metadata {
    version: u64,
    seq_id: u64,
    log_id: u64,
    part_id: u64,
    schema: Schema,
    // levels->parts
    parts: Vec<Vec<Part>>,
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
    AddField(Field),
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
    max_log_length: u64,
}

struct OptiDBImpl {
    opts: Options,
    memtable: SkipSet<MemOp>,
    metadata: Metadata,
    log: BufWriter<File>,
    path: PathBuf,
}

fn log_op(
    op: LogOp,
    recover: bool,
    log: Option<&mut File>,
    memtable: &mut SkipSet<MemOp>,
    metadata: &mut Metadata,
) -> Result<()> {
    trace!("log op: {:?}", op);
    if !recover {
        let data = serialize(&op)?;

        let crc = hash_crc32(&data);
        trace!("crc32: {}", crc);
        let log = log.unwrap();
        log.write_all(&crc.to_le_bytes())?;
        log.write_all(&(data.len() as u64).to_le_bytes())?;
        log.write_all(&data)?;

        let logged_size = 8 + 4 + data.len();
        trace!("logged size: {}", logged_size);
        metadata.stats.logged_bytes += logged_size as u64;
    }
    match op {
        LogOp::Insert(k, v) => {
            memtable.insert(MemOp {
                op: Op::Insert(k, v),
                seq_id: metadata.seq_id,
            });
        }
        LogOp::AddField(f) => {
            metadata.schema.fields.push(f);
        }
        LogOp::Metadata(md) => *metadata = md,
    }

    metadata.seq_id += 1;
    trace!("next op id: {}", metadata.seq_id);

    Ok(())
}

// #[instrument(level = "trace")]
fn recover(path: PathBuf, opts: Options) -> Result<OptiDBImpl> {
    trace!("db dir: {:?}", path);
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
        part_id: 0,
        schema: Schema::from(vec![]),
        parts: vec![],
        stats: Default::default(),
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

    trace!("metadata: {:?}", metadata);
    Ok(OptiDBImpl {
        opts,
        memtable,
        metadata,
        log: BufWriter::new(log),
        path,
    })
}

fn write_level0(
    memtable: &SkipSet<MemOp>,
    part_id: u64,
    schema: &Schema,
    path: PathBuf,
) -> Result<Part> {
    let mut cols: Vec<Vec<Value>> = vec![Vec::new(); schema.fields.len()];
    trace!("{} entries to write", memtable.len());
    for op in memtable {
        let (keys, vals) = match &op.op {
            Op::Insert(k, v) => (k, Some(v)),
            Op::Delete(k) => (k, None),
        };
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

    println!("{:?}", arrs);
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

    trace!("trying to create {:?}", path.join("parts/0"));
    fs::create_dir_all(path.join("parts/0"))?;

    let p = path.join("parts").join("0").join(part_id.to_string());
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
    })
}

impl OptiDBImpl {
    // #[instrument(level = "trace")]
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(path, opts)
    }
    // #[instrument(level = "trace", skip(self))]
    pub fn insert(&mut self, key: Vec<KeyValue>, values: Vec<Value>) -> Result<()> {
        if key.len() + values.len() != self.metadata.schema.fields.len() {
            return Err(StoreError::Internal(format!(
                "Fields mismatch. Key+Val len: {}, schema fields len: {}",
                key.len() + values.len(),
                self.metadata.schema.fields.len()
            )));
        }
        log_op(
            LogOp::Insert(key, values),
            false,
            Some(self.log.get_mut()),
            &mut self.memtable,
            &mut self.metadata,
        )?;
        if self.metadata.stats.logged_bytes > self.opts.max_log_length {
            println!("{}", self.metadata.stats.logged_bytes);
            self.flush()?;
        }
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
        for f in &self.metadata.schema.fields {
            if f.name == field.name {
                return Err(StoreError::Internal(format!(
                    "Field with name {} already exists",
                    field.name
                )));
            }
        }
        log_op(
            LogOp::AddField(field),
            false,
            Some(self.log.get_mut()),
            &mut self.memtable,
            &mut self.metadata,
        )?;
        Ok(())
    }

    // swap memtable
    // write memtable to parquet
    // update metadata
    // write metadata to new log
    // delete cur log
    // #[instrument(level = "trace", skip(self))]
    fn flush(&mut self) -> Result<()> {
        trace!("flushing log file");
        let f = self.log.get_mut();
        f.flush()?;
        f.sync_all()?;
        // swap memtable
        trace!("swapping memtable");
        let memtable = std::mem::take(&mut self.memtable);

        // write to parquet
        trace!("writing to parquet");
        let part = write_level0(
            &memtable,
            self.metadata.part_id,
            &self.metadata.schema,
            self.path.clone(),
        )?;
        if self.metadata.parts.len() == 0 {
            self.metadata.parts.push(Vec::new());
        }
        self.metadata.parts[0].push(part.clone());
        // increment table id
        self.metadata.part_id += 1;
        self.metadata.stats.written_bytes += part.size_bytes;
        // increment log id
        self.metadata.log_id += 1;

        // create new log
        trace!(
            "creating new log file {:?}",
            format!("{:016}.log", self.metadata.log_id)
        );

        let log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(self.path.join(format!("{:016}.log", self.metadata.log_id)))?;
        self.log = BufWriter::new(log);

        // write metadata to log as first record
        let op = LogOp::Metadata(self.metadata.clone());
        log_op(
            op,
            false,
            Some(self.log.get_mut()),
            &mut self.memtable,
            &mut self.metadata,
        )?;

        trace!(
            "removing previous log file {:?}",
            format!("{:016}.log", self.metadata.log_id - 1)
        );
        fs::remove_file(
            self.path
                .join(format!("{:016}.log", self.metadata.log_id - 1)),
        )?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::fs;
    use std::sync::Arc;

    use arrow2::datatypes::DataType;
    use arrow2::datatypes::Field;
    use arrow2::datatypes::Schema;
    use tracing::info;
    use tracing::log;
    use tracing::log::debug;
    use tracing::trace;
    use tracing::Level;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_test::traced_test;

    use crate::db::Insert;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::ColValue;
    use crate::KeyValue;
    use crate::Value;

    #[traced_test]
    #[test]
    fn it_works() {
        let path = temp_dir().join("db3");
        fs::create_dir_all(&path).unwrap();
        let opts = Options {
            max_log_length: 1024,
        };
        let mut db = OptiDBImpl::open(path, opts).unwrap();
        db.add_field(Field::new("a", DataType::Utf8, false));
        db.add_field(Field::new("b", DataType::Utf8, false));
        db.add_field(Field::new("c", DataType::Int8, false));
        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("b".to_string()),
            ],
            vec![Value::Int8(Some(1))],
        )
        .unwrap();
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("a".to_string()),
        // ],
        // vec![Value::Int8(Some(2))],
        // )
        // .unwrap();
        //
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("a".to_string()),
        // ],
        // vec![Value::Int8(Some(3))],
        // )
        // .unwrap();
        //
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("b".to_string()),
        // ],
        // vec![Value::Int8(Some(4))],
        // )
        // .unwrap();
        //
        // db.insert(
        // vec![
        // KeyValue::String("a".to_string()),
        // KeyValue::String("b".to_string()),
        // ],
        // vec![Value::Int8(Some(5))],
        // )
        // .unwrap();

        db.flush().unwrap();
    }
}
