use std::cmp;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time;
use std::time::Instant;

use tracing::error;

use crate::db::log_metadata;
use crate::db::part_path;
use crate::db::FsOp;
use crate::db::Level;
use crate::db::Metadata;
use crate::db::Options;
use crate::db::Part;
use crate::db::Vfs;
use crate::error::Result;
use crate::parquet::merger;
use crate::parquet::merger::parquet_merger;
use crate::parquet::merger::parquet_merger::merge;

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
    opts: Options,
    metadata: Arc<Mutex<Metadata>>,
    log: Arc<Mutex<BufWriter<File>>>,
    path: PathBuf,
    inbox: Receiver<CompactorMessage>,
    vfs: Arc<Vfs>,
}

impl Compactor {
    pub fn new(
        opts: Options,
        metadata: Arc<Mutex<Metadata>>,
        log: Arc<Mutex<BufWriter<File>>>,
        path: PathBuf,
        inbox: Receiver<CompactorMessage>,
        vfs: Arc<Vfs>,
    ) -> Self {
        Compactor {
            opts,
            metadata,
            log,
            path,
            inbox,
            vfs,
        }
    }
    pub fn run(mut self) {
        loop {
            match self.inbox.try_recv() {
                Ok(v) => {
                    match v {
                        CompactorMessage::Stop(dropper) => {
                            drop(dropper);
                            break;
                        }
                        _ => unreachable!(),
                    }
                    // !@#println!("Terminating.");
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
            // !@#debug!("compaction started");
            let partitions = {
                let md = self.metadata.lock().unwrap();
                md.partitions.clone()
            };
            // !@#println!("md lvlb {}", levels[0].part_id);
            // print_levels(&levels);
            for (pid, partition) in partitions.iter().enumerate() {
                let start = Instant::now();
                match compact(&partition.levels, pid, &self.path, &self.opts) {
                    Ok(res) => match res {
                        None => continue,
                        Some(res) => {
                            let mut md = self.metadata.lock().unwrap();
                            // !@#println!("md lvl");
                            // print_levels(&md.levels);
                            // print_levels(&md.levels);
                            for rem in &res.l0_remove {
                                md.partitions[pid].levels[0].parts = md.partitions[pid].levels[0]
                                    .parts
                                    .clone()
                                    .into_iter()
                                    .filter(|p| p.id != *rem)
                                    .collect::<Vec<_>>();
                            }
                            // !@#println!("res rem: {:?}", res.l0_remove);
                            // !@#println!("res levels");
                            // print_levels(&res.levels);
                            for (idx, l) in res.levels.iter().enumerate().skip(1) {
                                md.partitions[pid].levels[idx] = l.clone();
                            }
                            let mut log = self.log.lock().unwrap();
                            log_metadata(Some(log.get_mut()), &mut md).unwrap();
                            // drop because next fs operation is with locking
                            drop(md);
                            // !@#println!("md lvl after");
                            // print_levels(&md.levels);
                            for op in res.fs_ops {
                                match op {
                                    FsOp::Rename(from, to) => {
                                        // !@#trace!("renaming");
                                        // todo handle error
                                        self.vfs.rename(from, to).unwrap();
                                    }
                                    FsOp::Delete(path) => {
                                        // !@#trace!("deleting {:?}",path);
                                        self.vfs.remove_file(path).unwrap();
                                    }
                                }
                            }
                            // !@#println!("After");
                            // let mut md = self.metadata.lock().unwrap();
                            // // !@#println!("post md");
                            // print_levels(&md.levels);
                        }
                    },
                    Err(err) => {
                        error!("compaction error: {:?}", err);

                        continue;
                    }
                }
            }
            // !@#debug!("compaction finished in {:?}", duration);
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
    opts: &Options,
) -> Result<Option<Vec<ToCompact>>> {
    // !@#println!("lid {} {}", level_id, level.parts.len());
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

fn compact(
    levels: &[Level],
    partition_id: usize,
    path: &PathBuf,
    opts: &Options,
) -> Result<Option<CompactResult>> {
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
                // !@#println!("to compact {:?}", to_compact);
                if to_compact.len() == 1 {
                    // !@#println!("rename");

                    let idx = tmp_levels[l]
                        .parts
                        .iter()
                        .position(|p| p.id == to_compact[0].part)
                        .unwrap();

                    let mut part = tmp_levels[l].parts[idx].clone();
                    tmp_levels[l].parts.remove(idx);
                    part.id = tmp_levels[l + 1].part_id + 1;

                    let from = part_path(&path, partition_id, l, to_compact[0].part);
                    let to = part_path(&path, partition_id, l + 1, part.id);
                    fs_ops.push(FsOp::Rename(from, to));

                    tmp_levels[l + 1].parts.push(part.clone());
                    tmp_levels[l + 1].part_id += 1;
                    // !@#println!("done");
                    continue;
                }
                // !@#println!("llee {}", to_compact.len());
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

                let in_paths = to_merge
                    .iter()
                    .map(|p| part_path(&path, partition_id, l, p.id))
                    .collect::<Vec<_>>();
                // !@#println!("{:?}", in_paths);
                let out_part_id = tmp_levels[l + 1].part_id + 1;
                let out_path = path.join(format!("parts/{}/{}", partition_id, l + 1));
                let rdrs = in_paths
                    .iter()
                    .map(|p| File::open(p))
                    .collect::<std::result::Result<Vec<File>, std::io::Error>>()?;
                let merger_opts = parquet_merger::Options {
                    index_cols: opts.merge_index_cols,
                    data_page_size_limit_bytes: opts.merge_data_page_size_limit_bytes,
                    row_group_values_limit: opts.merge_row_group_values_limit,
                    array_page_size: opts.merge_array_page_size,
                    out_part_id,
                    max_part_size_bytes: Some(opts.merge_max_part_size_bytes),
                };
                let merge_result = merge(rdrs, out_path, out_part_id, merger_opts)?;
                // !@#println!("merge result {:#?}", merge_result);
                for f in merge_result {
                    let final_part = {
                        Part {
                            id: tmp_levels[l + 1].part_id + 1,
                            size_bytes: f.size_bytes,
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
                    tmp_levels[l + 1].parts.push(final_part);
                    tmp_levels[l + 1].part_id += 1;
                }

                fs_ops.append(
                    in_paths
                        .iter()
                        .map(|p| FsOp::Delete(p.clone()))
                        .collect::<Vec<_>>()
                        .as_mut(),
                );
                // !@#println!("after compaction");
            }
        }
    }

    // !@#println!("return lvl");
    Ok(Some(CompactResult {
        l0_remove: l0_rem,
        levels: tmp_levels,
        fs_ops,
    }))
}
