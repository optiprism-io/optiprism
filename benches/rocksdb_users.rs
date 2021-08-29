use serde::{Serialize, Deserialize};
use rocksdb::{DB, Options, ColumnFamilyDescriptor, IteratorMode};
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use byteorder::{ReadBytesExt, LittleEndian};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Row {
    cols: Vec<u64>,
}

fn bincode_insert(c: &mut Criterion) {
    let mut i: u64 = 0;

    for cols in vec![5, 10, 20] {
        let path = format!("/tmp/bincode_insert_{}", cols);
        let db = DB::open_default(path.clone()).unwrap();
        c.bench_with_input(BenchmarkId::new("bincode insert", format!("{} cols", cols)), &cols, |b, cc| {
            b.iter(|| {
                let c = i % 1_000_000;
                let mut w = Row {
                    cols: vec![0; *cc]
                };

                for j in 0..*cc {
                    w.cols[j] = c
                }
                let e: Vec<u8> = bincode::serialize(&w).unwrap();
                db.put(i.to_le_bytes(), e).unwrap();
                i += 1;
            })
        });
        let _ = DB::destroy(&Options::default(), path.clone());
    }
}

fn cf_insert(c: &mut Criterion) {
    for cols in vec![5, 10, 20] {
        let path = format!("/tmp/cf_insert{}", cols);
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let mut cfs: Vec<ColumnFamilyDescriptor> = vec![];

        let mut i: u64 = 0;

        for i in 0..cols {
            cfs.push(ColumnFamilyDescriptor::new(i.to_string(), cf_opts.clone()));
        }

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = DB::open_cf_descriptors(&db_opts, path.clone(), cfs).unwrap();

        c.bench_with_input(BenchmarkId::new("cf insert", format!("{} cols", cols)), &cols, |b, cc| {
            b.iter(|| {
                let c = i % 1_000_000;
                for j in 0..*cc {
                    db.put_cf(db.cf_handle(&j.to_string()).unwrap(), c.to_le_bytes(), c.to_le_bytes()).unwrap();
                }
                i += 1;
            })
        });

        let _ = DB::destroy(&Options::default(), path.clone());
    }
}

fn bincode_query(c: &mut Criterion) {
    for cols in vec![1, 3, 5] {
        let path = format!("/tmp/bincode_query_{}", cols);
        let db = DB::open_default(path.clone()).unwrap();

        let mut w = Row {
            cols: vec![0; 50]
        };
        for i in 0..100_000i64 {
            for j in 0..5 {
                w.cols[j] = i as u64;
            }
            let e: Vec<u8> = bincode::serialize(&w).unwrap();
            db.put(i.to_le_bytes(), e).unwrap();
        }

        let mut iter = db.iterator(IteratorMode::Start);
        c.bench_with_input(BenchmarkId::new("bincode query", format!("{} cols", cols)), &cols, |b, cc| {
            b.iter(|| {
                for j in 0..1000 {
                    match iter.next() {
                        None => {
                            iter = db.iterator(IteratorMode::Start);
                        }
                        Some((k, v)) => {
                            black_box(bincode::deserialize::<Row>(v.as_ref()).unwrap());
                        }
                    }
                }
            })
        });
        let _ = DB::destroy(&Options::default(), path.clone());
    }
}

fn cf_query(c: &mut Criterion) {
    for cols in vec![1, 3, 5] {
        let path = format!("/tmp/cf_query_{}", cols);
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        let mut cfs: Vec<ColumnFamilyDescriptor> = vec![];

        for i in 0..cols {
            cfs.push(ColumnFamilyDescriptor::new(i.to_string(), cf_opts.clone()));
        }

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);
        let db = DB::open_cf_descriptors(&db_opts, path.clone(), cfs).unwrap();

        for i in 0..100_000i64 {
            for j in 0..cols {
                db.put_cf(db.cf_handle(&j.to_string()).unwrap(), i.to_le_bytes(), i.to_le_bytes()).unwrap();
            }
        }

        let mut iters = vec![];
        for i in 0..cols {
            iters.push(db.iterator_cf(db.cf_handle(&i.to_string()).unwrap(), IteratorMode::Start));
        }

        c.bench_with_input(BenchmarkId::new("cf query", format!("{} cols", cols)), &cols, |b, cc| {
            b.iter(|| {
                for i in 0..cols {
                    for _ in 0..1000 {
                        match iters[i].next() {
                            None => {
                                iters[i] = db.iterator_cf(db.cf_handle(&i.to_string()).unwrap(), IteratorMode::Start);
                            }
                            Some((k, v)) => {
                                black_box(v.as_ref().read_u64::<LittleEndian>().unwrap());
                            }
                        }
                    }
                }
            })
        });
        let _ = DB::destroy(&Options::default(), path.clone());
    }
}

fn raw_vec_query(c: &mut Criterion) {
    for cols in vec![1, 3, 5] {
        let db = vec![vec![0u64; 100_000]; cols];

        let mut iters = vec![];
        for i in 0..cols {
            iters.push(db[i].iter());
        }

        c.bench_with_input(BenchmarkId::new("raw vec query", format!("{} cols", cols)), &cols, |b, cc| {
            b.iter(|| {
                for i in 0..cols {
                    for _ in 0..1000 {
                        match iters[i].next() {
                            None => {
                                iters[i] = db[i].iter();
                            }
                            Some(v) => {
                                black_box(*v);
                            }
                        }
                    }
                }
            })
        });
    }
}
criterion_group!(benches, /*bincode_insert,cf_insert,*/bincode_query,cf_query,raw_vec_query);
criterion_main!(benches);