use std::fmt::Write;
use std::{fs, thread};
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use arrow2::array::{Array, Int64Array};
use clap::Parser;
use futures::{Stream, StreamExt};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rand::Rng;
use tokio::time::Instant;
use common::startup_config::StartupConfig;
use common::{DATA_PATH_METADATA, DATA_PATH_STORAGE};
use common::types::DType;
use storage::db::{OptiDBImpl, Options};
use storage::{NamedValue, table, Value};
use storage::parquet::ArrowIteratorImpl;
use crate::{init_metrics, init_system};
#[derive(Parser, Clone)]
pub struct Gen {
    #[arg(long)]
    records: usize,
}
#[derive(Parser, Clone)]
pub struct Query {}
#[derive(Parser, Clone)]
pub enum Commands {
    Gen(Gen),
    Query(Query),
}
#[derive(Parser, Clone)]
pub struct DbTest {
    #[arg(long)]
    pub path: PathBuf,
    #[arg(long)]
    pub config: PathBuf,
    #[command(subcommand)]
    pub cmd: Commands,
}

pub async fn gen(args: &DbTest, gen: &Gen) -> crate::error::Result<()> {
    init_metrics();
    fs::remove_dir_all(args.path.join(DATA_PATH_STORAGE))?;
    let db = Arc::new(OptiDBImpl::open(args.path.join(DATA_PATH_STORAGE), Options {})?);
    let topts = table::Options {
        levels: 7,
        merge_array_size: 10000,
        index_cols: 1,
        l1_max_size_bytes: 1024 * 1024 * 100,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024 * 5,
        merge_array_page_size: 10000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024),
        merge_max_l1_part_size_bytes: 1024 * 1024,
        merge_part_size_multiplier: 5,
        merge_row_group_values_limit: 1000000,
        merge_chunk_size: 1024 * 8 * 8,
        merge_max_page_size: 1024 * 1024 * 10,
        is_replacing: false,
    };
    db.create_table("t1".to_string(), topts.clone()).unwrap();
    db.add_field("t1", "f1", DType::Int64, false).unwrap();
    db.add_field("t1", "f2", DType::Int16, false).unwrap();
    db.add_field("t1", "f3", DType::Int32, true).unwrap();
    db.add_field("t1", "f4", DType::Int64, true).unwrap();
    db.add_field("t1", "f5", DType::String, true).unwrap();
    db.add_field("t1", "f6", DType::Decimal, true).unwrap();
    db.add_field("t1", "f7", DType::Boolean, true).unwrap();
    db.add_field("t1", "f8", DType::Timestamp, true).unwrap();


    let pb = ProgressBar::new(gen.records as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} records ({eta})",
        )
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
            })
            .progress_chars("#>-"),
    );
    let db_cloned = db.clone();
    /*thread::spawn(move || {
        loop {
            db_cloned.full_backup_local("/tmp/bak",|pct|{
                dbg!(pct);
            }).unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });*/

    let db_cloned = db.clone();
    thread::spawn(move || {
        loop {
            db_cloned.flush("t1").unwrap();
            thread::sleep(Duration::from_secs(1));
        }
    });


    /*    let db_cloned = db.clone();
        thread::spawn(move || {
            loop {
                db_cloned.full_restore_local("/tmp/bak").unwrap();
                thread::sleep(Duration::from_secs(1));
                println!("restore");
            }
        });*/
    let recs = gen.records;
    let mut hnd = vec![];
    for i in 0..=2 {
        let pb_cloned = pb.clone();
        let db_cloned = db.clone();
        let h = thread::spawn(move || {
            for j in 0..recs {
                if j % 3 != i {
                    continue;
                }
                db_cloned.insert(
                    "t1",
                    vec![
                        NamedValue::new("f1".to_string(), Value::Int64(Some(10000000 - i as i64))),
                        // NamedValue::new("f1".to_string(), Value::Int64(Some(rng.gen_range(0..10000000)))),
                        NamedValue::new("f2".to_string(), Value::Int16(Some(i as i16))),
                        NamedValue::new("f3".to_string(), Value::Int32(Some(i as i32))),
                        NamedValue::new("f4".to_string(), Value::Int64(Some(i as i64))),
                        NamedValue::new("f5".to_string(), Value::String(Some(i.to_string()))),
                        NamedValue::new("f6".to_string(), Value::Decimal(Some(i as i128))),
                        NamedValue::new("f7".to_string(), Value::Boolean(Some(i % 2 == 0))),
                        NamedValue::new("f8".to_string(), Value::Timestamp(Some(i as i64))),
                    ],
                ).unwrap();
                pb_cloned.inc(1);
            }
        });
        hnd.push(h);
    }
    for h in hnd {
        h.join().unwrap();
    }
    Ok(())
}

pub async fn query(args: &DbTest, q: &Query) -> crate::error::Result<()> {
    use std::fs::File;

    use arrow2::array::Array;
    /*
        let a = Instant::now();
        let i = ArrowIteratorImpl::new(BufReader::new(File::open("/tmp/storage/data/tables/t1/levels/1/31.parquet").unwrap()), vec!["f1".to_string()], 10000).unwrap();
        let mut c = 0;
        for v in i {
            println!("!");
            let v=v.unwrap();
            c+=v.len();
        }
        println!("{c}");
        println!("{:?}", a.elapsed());
        panic!();*/
    let db = Arc::new(OptiDBImpl::open(args.path.join(DATA_PATH_STORAGE), Options {})?);

    let s = Instant::now();
    let mut v = 0;
    let mut scan = db.scan("t1", vec![0, 1, 2, 3, 4, 5, 6, 7])?;
    for i in scan.iter {
        v += i.unwrap().len();
    }
    dbg!(v);
    dbg!(s.elapsed());
    let s = Instant::now();
    let mut v = 0;
    let mut i = 0;
    /*loop {
        match scan.next().await {
            None => {
                break;
            }
            Some(Ok(chunk)) => {
                // v += chunk.len();
                let arr = chunk.arrays()[0].as_any().downcast_ref::<Int64Array>().unwrap();
                i = arr.value(arr.len() - 1);
            }

            Some(Err(e)) => { return Err(e.into()) }
        }
    }
*/
    dbg!(s.elapsed());
    dbg!(v);
    dbg!(i + 1);
    Ok(())
}