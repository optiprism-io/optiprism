use std::cmp;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
#[cfg(not(test))]
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
#[cfg(not(test))]
use std::thread;
#[cfg(not(test))]
use std::time::Duration;
use std::time::Instant;

use log::trace;
use metrics::counter;
use metrics::histogram;
use parking_lot::RwLock;

use crate::db::log_metadata;
use crate::error::Result;
use crate::parquet::parquet_merger;
use crate::parquet::parquet_merger::merge;
use crate::table;
use crate::table::part_path;
use crate::table::Level;
use crate::table::Part;
use crate::table::Table;
use crate::FsOp;

#[derive(Clone, Debug)]
pub enum CompactorMessage {
    Compact,
    Stop(Sender<()>),
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

struct CompactResult {
    l0_remove: Vec<usize>,
    levels: Vec<Level>,
    fs_ops: Vec<FsOp>,
}

pub struct Compactor {
    tables: Arc<RwLock<Vec<Table>>>,
    path: PathBuf,
    inbox: Receiver<CompactorMessage>,
}

impl Compactor {
    pub fn new(
        tables: Arc<RwLock<Vec<Table>>>,
        path: PathBuf,
        inbox: Receiver<CompactorMessage>,
    ) -> Self {
        Compactor {
            tables,
            path,
            inbox,
        }
    }
    pub fn run(self) {
        loop {
            #[cfg(not(test))]
            {
                match self.inbox.try_recv() {
                    Ok(v) => match v {
                        CompactorMessage::Stop(dropper) => {
                            drop(dropper);
                            break;
                        }
                        _ => unreachable!(),
                    },
                    Err(TryRecvError::Disconnected) => {
                        break;
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }
            #[cfg(not(test))]
            thread::sleep(Duration::from_micros(20)); // todo make configurable

            #[cfg(test)]
            {
                match self.inbox.recv() {
                    Ok(msg) => match msg {
                        CompactorMessage::Compact => {}
                        CompactorMessage::Stop(dropper) => {
                            drop(dropper);
                            break;
                        }
                    },
                    Err(err) => panic!("{:?}", err),
                }
            }
            // !@#debug!("compaction started");
            let tables = {
                let tbls = self.tables.read();
                tbls.clone()
            };

            for table in tables {
                let metadata = {
                    let md = table.metadata.lock();
                    md.clone()
                };
                for (pid, partition) in metadata.partitions.iter().enumerate() {
                    let _start = Instant::now();
                    match compact(
                        table.name.as_str(),
                        partition.levels.clone(),
                        pid,
                        &self.path,
                        &metadata.opts,
                    ) {
                        Ok(res) => match res {
                            None => continue,
                            Some(res) => {
                                let mut metadata = table.metadata.lock();
                                for rem in &res.l0_remove {
                                    metadata.partitions[pid].levels[0].parts =
                                        metadata.partitions[pid].levels[0]
                                            .parts
                                            .clone()
                                            .into_iter()
                                            .filter(|p| p.id != *rem)
                                            .collect::<Vec<_>>();
                                }
                                for (idx, l) in res.levels.iter().enumerate().skip(1) {
                                    metadata.partitions[pid].levels[idx] = l.clone();
                                }
                                let mut log = table.log.lock();
                                log_metadata(log.get_mut(), &mut metadata).unwrap();
                                drop(metadata);
                                // drop because next fs operation is with locking
                                for op in res.fs_ops {
                                    match op {
                                        FsOp::Rename(from, to) => {
                                            trace!("renaming");
                                            // todo handle error
                                            table.vfs.rename(from, to).unwrap();
                                        }
                                        FsOp::Delete(path) => {
                                            trace!("deleting {:?}", path);
                                            table.vfs.remove_file(path).unwrap();
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            panic!("compaction error: {:?}", err);
                        }
                    }
                }
            }
        }
        //     CompactorMessage::Stop(dropper) => {
        //         drop(dropper);
        //         break;
        //     }
        // },
        // Err(err) => {
        //     // !@#trace!("unexpected compactor error: {:?}", err);
        //     break;
        // }
        // }
    }
}

fn determine_compaction(
    level_id: usize,
    level: &Level,
    next_level: &Level,
    opts: &table::Options,
) -> Result<Option<Vec<ToCompact>>> {
    let mut to_compact = vec![];

    let level_parts = &level.parts;
    if level_id == 0 && level_parts.len() > opts.l0_max_parts {
        for part in level_parts {
            to_compact.push(ToCompact::new(0, part.id));
        }
    } else if level_id > 0 && !level_parts.is_empty() {
        let mut size = 0;
        for part in level_parts {
            size += part.size_bytes;
            let level_threshold =
                opts.l1_max_size_bytes * opts.level_size_multiplier.pow(level_id as u32 - 1);
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

fn compact(
    tbl_name: &str,
    levels: Vec<Level>,
    partition_id: usize,
    path: &Path,
    opts: &table::Options,
) -> Result<Option<CompactResult>> {
    let init_time = Instant::now();
    let mut fs_ops = vec![];
    let mut l0_rem: Vec<usize> = Vec::new();
    let mut tmp_levels = levels.clone();
    let mut compacted = false;
    for level_id in 0..tmp_levels.len() - 2 {
        let v = determine_compaction(
            level_id,
            &tmp_levels[level_id],
            &tmp_levels[level_id + 1],
            opts,
        )?;
        match v {
            None => {
                if compacted {
                    break;
                } else {
                    return Ok(None);
                }
            }
            Some(to_compact) => {
                let start_time = Instant::now();

                compacted = true;
                if to_compact.len() == 1 {
                    let idx = tmp_levels[level_id]
                        .parts
                        .iter()
                        .position(|p| p.id == to_compact[0].part)
                        .unwrap();

                    let mut part = tmp_levels[level_id].parts[idx].clone();
                    tmp_levels[level_id].parts.remove(idx);
                    part.id = tmp_levels[level_id + 1].part_id + 1;

                    let from =
                        part_path(path, tbl_name, partition_id, level_id, to_compact[0].part);
                    let to = part_path(path, tbl_name, partition_id, level_id + 1, part.id);
                    fs_ops.push(FsOp::Rename(from, to));

                    tmp_levels[level_id + 1].parts.push(part.clone());
                    tmp_levels[level_id + 1].part_id += 1;
                    continue;
                }
                let mut tomerge = vec![];
                for tc in &to_compact {
                    if tc.level == 0 {
                        l0_rem.push(tc.part);
                    }
                    tomerge.push(part_path(path, tbl_name, partition_id, tc.level, tc.part));
                    tmp_levels[tc.level].parts = tmp_levels[tc.level]
                        .clone()
                        .parts
                        .into_iter()
                        .filter(|p| p.id != tc.part)
                        .collect::<Vec<_>>();
                }

                let out_part_id = tmp_levels[level_id + 1].part_id + 1;
                let out_path = path.join(format!(
                    "tables/{}/{}/{}",
                    tbl_name,
                    partition_id,
                    level_id + 1
                ));
                let rdrs = tomerge
                    .iter()
                    .map(File::open)
                    .collect::<std::result::Result<Vec<File>, std::io::Error>>()
                    .unwrap();
                let max_part_size_bytes = opts.merge_max_l1_part_size_bytes
                    * opts.merge_part_size_multiplier.pow(level_id as u32 + 1);
                let merger_opts = parquet_merger::Options {
                    index_cols: opts.merge_index_cols,
                    data_page_size_limit_bytes: opts.merge_data_page_size_limit_bytes,
                    row_group_values_limit: opts.merge_row_group_values_limit,
                    array_page_size: opts.merge_array_page_size,
                    out_part_id,
                    merge_max_page_size: opts.merge_max_page_size,
                    max_part_size_bytes: Some(max_part_size_bytes),
                };
                let merge_result =
                    merge(rdrs, out_path, out_part_id, tbl_name, level_id, merger_opts)?;
                for f in merge_result {
                    let final_part = {
                        Part {
                            id: tmp_levels[level_id + 1].part_id + 1,
                            size_bytes: f.size_bytes,
                            values: f.values,
                            min: f
                                .min
                                .iter()
                                .map(|v| v.try_into())
                                .collect::<Result<Vec<_>>>()?,
                            max: f
                                .max
                                .iter()
                                .map(|v| v.try_into())
                                .collect::<Result<Vec<_>>>()?,
                        }
                    };
                    tmp_levels[level_id + 1].parts.push(final_part);
                    tmp_levels[level_id + 1].part_id += 1;
                }

                fs_ops.append(
                    tomerge
                        .iter()
                        .map(|p| FsOp::Delete(p.clone()))
                        .collect::<Vec<_>>()
                        .as_mut(),
                );
                histogram!("store.level_compaction_time_seconds","table"=>tbl_name.to_string(),"level"=>level_id.to_string()).record(start_time.elapsed());
            }
        }
    }
    counter!("store.compactions_total","table"=>tbl_name.to_string()).increment(1);
    histogram!("store.compaction_time_seconds","table"=>tbl_name.to_string())
        .record(init_time.elapsed());

    Ok(Some(CompactResult {
        l0_remove: l0_rem,
        levels: tmp_levels,
        fs_ops,
    }))
}
