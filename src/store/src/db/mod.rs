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
use std::os::unix::fs::DirEntryExt2;
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

struct Table {
    id: u64,
    size_bytes: u64,
}

#[derive(Debug, Default, Serialize, Deserialize, Hash)]
struct Metadata {
    version: u64,
    seq_id: u64,
    log_id: u64,
    table_id: u64,
    // levels->tables
    tables: Vec<Vec<Table>>,
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
    Op(Op),
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

#[derive(Debug, Clone, Copy, Default)]
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

#[derive(Clone)]
struct Options {
    max_log_length: usize,
    schema: SchemaRef,
}

struct OptiDBImpl {
    opts: Options,
    memtable: SkipSet<MemOp>,
    metadata: Metadata,
    log: BufWriter<File>,
    schema: SchemaRef,
    path: PathBuf,
}

fn load_metadata(path: PathBuf) {}

fn recover(path: PathBuf, opts: Options) -> Result<OptiDBImpl> {
    let dir = fs::read_dir(path.clone())?;
    let mut logs = BinaryHeap::new();
    for f in dir {
        let f = f?;
        match f.path().extension() {
            None => false,
            Some(n) => {
                if n == OsStr::new("log") {
                    logs.push(f)
                }
            }
        };
    }

    let mut metadata = Default::default();
    let mut memtable = SkipSet::new();
    let log = if let Some(log) = logs.pop() {
        let mut f = BufReader::new(File::open(log.path())?);

        let mut crc_b = [0u8; mem::size_of::<i32>()];
        f.read_exact(&mut crc_b)?;
        let crc32 = i32::from_le_bytes(crc_b);

        let mut len_b = [0u8; mem::size_of::<u64>()];
        f.read_exact(&mut len_b)?;
        let len = i32::from_le_bytes(len_b);

        let mut data_b = Vec::with_capacity(len as usize);
        f.read_exact(&mut data_b)?;
        // todo recover from this case
        if crc32 != hash_crc32(&data_b) {
            return Err(StoreError::Internal("corrupted log".into()));
        }
        let log_op = deserialize::<LogOp>(&data_b)?;
        match log_op {
            LogOp::Op(op) => {
                memtable.insert(MemOp {
                    op,
                    seq_id: metadata.seq_id,
                });
                metadata.seq_id += 1;
            }
            LogOp::Metadata(md) => metadata = md,
        }

        drop(f);

        OpenOptions::new().write(true).read(true).open(log.path())?
    } else {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(path.join("0.log"))?
    };

    if recover {}
    Ok(OptiDBImpl {
        opts,
        memtable,
        metadata,
        log,
        schema: Arc::new(Default::default()),
        path,
    })
}

impl OptiDBImpl {
    pub fn open(path: PathBuf, opts: Options) -> Result<Self> {
        recover(&path, &opts)?;
        Ok(OptiDBImpl {
            opts: opts.clone(),
            memtable: Default::default(),
            metadata: Default::default(),
            log_id: 0,
            logged_bytes: 0,
            log: OpenOptions::new()
                .create_new(true)
                .write(true)
                .read(true)
                .open(path.join("log"))?,
            schema: opts.schema.clone(),
            path,
        })
    }
    pub fn insert(&mut self, key: Vec<KeyValue>, values: Vec<Value>) -> Result<()> {
        let op = Op::Insert(key, values);
        self.log_op(LogOp::Op(op))?;
        if self.metadata.stats.logged_bytes > self.opts.max_log_length {
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

    // 1. write to operation log
    // 2. insert to memtable
    fn log_op(&mut self, op: LogOp) -> Result<()> {
        let data = serialize(&op)?;

        let crc = hash_crc32(&data);
        self.log.write_all(&crc.to_le_bytes())?;
        self.log.write_all(&(data.len() as u64).to_le_bytes())?;
        self.log.write_all(&data)?;

        match op {
            LogOp::Op(op) => {
                self.memtable.insert(MemOp {
                    op,
                    seq_id: self.metadata.seq_id,
                });
            }
            LogOp::Metadata(md) => {}
        }

        self.metadata.seq_id += 1;
        let logged_size = 8 + 4 + data.len();
        self.logged_bytes += logged_size;
        self.metadata.stats.logged_bytes += logged_size as u64;

        Ok(())
    }

    // swap memtable
    // write memtable to parquet
    // update metadata
    // write metadata to new log
    // delete cur log
    fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        self.log.sync_all()?;

        // swap memtable
        let memtable = std::mem::take(&mut self.memtable);

        // write to parquet
        let mut cols: Vec<Vec<Value>> = vec![Vec::new(); self.schema.fields.len()];
        for op in memtable {
            let (keys, vals) = match op.op {
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
            .map(|(idx, col)| match self.schema.fields[idx].data_type {
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
                            _ => unreachable!(),
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

        let encodings = self
            .schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, |_| parquet::write::Encoding::Plain))
            .collect();

        let row_groups = parquet::write::RowGroupIterator::try_new(
            vec![Ok(chunk)].into_iter(),
            &self.schema,
            popts,
            encodings,
        )?;

        fs::create_dir_all(self.path.join("parts/0"))?;

        let p = self
            .path
            .join("parts")
            .join("0")
            .join(self.metadata.table_id.to_string());
        let w = OpenOptions::new().create_new(true).write(true).open(p)?;
        let mut writer = parquet::write::FileWriter::try_new(
            w,
            Schema::from(self.schema.fields.clone()),
            popts,
        )?;

        // Write the part
        for group in row_groups {
            writer.write(group?)?;
        }
        let sz = writer.end(None)?;

        // add table to md
        self.metadata.tables[0].push(Table {
            id: self.metadata.table_id,
            size_bytes: sz,
        });

        // increment table id
        self.metadata.table_id += 1;
        self.metadata.stats.written_bytes += sz;
        // increment log id
        self.metadata.log_id += 1;

        // create new log
        let log = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(self.path.join(format!("{:16x}.log", self.metadata.log_id)))?;
        self.log = log;

        // write metadata to log as first record
        let op = LogOp::Metadata(self.metadata.clone());
        self.log_op(op)?;

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

    use crate::db::Insert;
    use crate::db::MemOp;
    use crate::db::Op;
    use crate::db::OptiDBImpl;
    use crate::db::Options;
    use crate::ColValue;
    use crate::KeyValue;
    use crate::Value;

    #[test]
    fn it_works() {
        let path = temp_dir().join("db3");
        fs::create_dir_all(&path).unwrap();
        let opts = Options {
            max_log_length: 1024,
            schema: Arc::new(Schema::from(vec![
                Field::new("a", DataType::Utf8, false),
                Field::new("b", DataType::Utf8, false),
                Field::new("c", DataType::Int8, false),
            ])),
        };

        let mut db = OptiDBImpl::open(path, opts).unwrap();

        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("b".to_string()),
            ],
            vec![Value::Int8(Some(1))],
        )
        .unwrap();

        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("a".to_string()),
            ],
            vec![Value::Int8(Some(2))],
        )
        .unwrap();

        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("a".to_string()),
            ],
            vec![Value::Int8(Some(3))],
        )
        .unwrap();

        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("b".to_string()),
            ],
            vec![Value::Int8(Some(4))],
        )
        .unwrap();

        db.insert(
            vec![
                KeyValue::String("a".to_string()),
                KeyValue::String("b".to_string()),
            ],
            vec![Value::Int8(Some(5))],
        )
        .unwrap();

        db.flush().unwrap();
    }
}
