use std::collections::HashMap;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::io::{BufReader, ErrorKind, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use byteorder::{LittleEndian, NativeEndian, WriteBytesExt};
use rayon::iter::Either;
use rayon::prelude::*;

use parallel_hnsw::parameters::{OptimizationParameters, SearchParameters};
use parallel_hnsw::{keepalive, Serializable, VectorId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use tokio::task::block_in_place;
use vectorlink::indexer::{create_index_name, index_serialization_path};
use vectorlink::openai::Model;
use vectorlink::vectors::VectorStore;
use vectorlink::{batch::index_domain, configuration::HnswConfiguration};
use vectorlink_task::keepalive_sync;
use vectorlink_task::task::{SyncTaskLiveness, TaskHandler, TaskLiveness};

use parallel_hnsw::progress::{Interrupt, LayerStatistics, ProgressMonitor};

use std::fs::{File, OpenOptions};

#[derive(Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    domain: usize,
    commit: String,
    directory: String,
    segment_start: usize,
    segment_vector_count: usize,
    segment_count: usize,
    output_dir: String,
    distance_threshold: f32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct SearchProgress {
    segment_count: usize,
}

pub struct VectorlinkTaskHandler;

#[async_trait]
impl TaskHandler for VectorlinkTaskHandler {
    type Init = SearchRequest;

    // TODO: actual progress should not be an arbitrary json object but a meaningful serializable state object.
    type Progress = SearchProgress;

    type Complete = ();

    type Error = String;

    async fn initialize(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        Ok(SearchProgress { segment_count: 0 })
    }
    async fn process(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error> {
        let key = "fake";
        let request: SearchRequest = live.init().unwrap().unwrap();
        let SearchRequest {
            domain,
            commit,
            directory,
            segment_start,
            segment_vector_count,
            segment_count,
            output_dir,
            distance_threshold,
        } = request;
        let _state = live.progress().unwrap();
        let mut live = live.into_sync().unwrap();

        block_in_place(|| {
            let store = VectorStore::new(&directory, 1234);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                &directory,
                create_index_name(&format!("{domain}"), &commit)
            ));

            let hnsw = keepalive_sync!(
                live,
                HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap()
            );
            let sp = SearchParameters::default();

            // TODO: this needs to loop through multiple segments
            let output_dir_path: PathBuf = output_dir.into();
            for segment_index in segment_start..segment_start + segment_count {
                let iter = open_vector_segment(&directory, segment_index, segment_vector_count);
                let result_file_name = format!("{domain}_{segment_index}.queues");
                let result_index_name = format!("{domain}_{segment_index}.index");
                let mut result_file =
                    BufWriter::new(File::create(output_dir_path.join(result_file_name)).unwrap());
                let mut result_index =
                    BufWriter::new(File::create(output_dir_path.join(result_index_name)).unwrap());
                result_index.write_u64::<NativeEndian>(0).unwrap();
                let record_len = std::mem::size_of::<(VectorId, f32)>();
                for v in iter {
                    let mut result =
                        hnsw.search_1024(parallel_hnsw::AbstractVector::Unstored(&v), sp);
                    let result_count = result
                        .iter()
                        .position(|(_, distance)| *distance > distance_threshold)
                        .unwrap_or(result.len());
                    result.truncate(result_count);
                    // And now do something with that result
                    let data_len = record_len * result.len();
                    result_index
                        .write_u64::<NativeEndian>(data_len as u64)
                        .unwrap();
                    unsafe {
                        let data_slice =
                            std::slice::from_raw_parts(result.as_ptr() as *const u8, data_len);
                        result_file.write_all(data_slice).unwrap();
                    }
                }

                result_file.flush().unwrap();
                result_file.get_ref().sync_all().unwrap();
                result_index.flush().unwrap();
                result_index.get_ref().sync_all().unwrap();
            }
        });

        Ok(())
    }
}

fn open_vector_segment<P: AsRef<Path>>(
    directory: P,
    segment_index: usize,
    segment_vector_count: usize,
) -> impl Iterator<Item = [f32; 1024]> {
    let mut domain_index = 0;
    let dir_path: &Path = directory.as_ref();
    loop {
        let path = dir_path.join(format!("{domain_index}.vecs"));
        let size_in_bytes = std::fs::metadata(&path).unwrap().size() as usize;
        let size_in_vecs = size_in_bytes / 4096;
        let mut start = segment_index * segment_vector_count;
        if size_in_vecs >= start {
            start -= size_in_vecs;
            domain_index += 1;
            continue;
        } else {
            let mut file = File::open(path).unwrap();
            file.seek(SeekFrom::Start(start as u64 * 4096)).unwrap();

            return VectorIterator {
                remaining_vecs: segment_vector_count,
                remaining_vecs_in_file: size_in_vecs - start,
                file: BufReader::new(file),
                dir_path: dir_path.into(),
                index: domain_index,
            };
        }
    }
}

pub struct VectorIterator {
    remaining_vecs: usize,
    remaining_vecs_in_file: usize,
    file: BufReader<File>,
    dir_path: PathBuf,
    index: usize,
}

impl Iterator for VectorIterator {
    type Item = [f32; 1024];

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_vecs == 0 {
            return None;
        }
        if self.remaining_vecs_in_file == 0 {
            // open next file
            self.index += 1;

            let path = self.dir_path.join(format!("{}.vecs", self.index));
            let file = File::open(path).unwrap();
            self.remaining_vecs_in_file = file.metadata().unwrap().size() as usize;
            self.file = BufReader::new(file);
        }

        let mut result = [0_u8; 4096];
        self.file.read_exact(&mut result).unwrap();
        self.remaining_vecs_in_file -= 1;
        self.remaining_vecs -= 1;
        Some(unsafe { std::mem::transmute(result) })
    }
}

#[cfg(test)]
mod tests {}
