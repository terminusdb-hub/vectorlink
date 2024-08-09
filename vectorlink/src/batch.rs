use std::{
    io::{self, SeekFrom},
    os::unix::prelude::MetadataExt,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
};

use futures::{future, Stream, StreamExt, TryStreamExt};
use parallel_hnsw::{
    keepalive,
    parameters::{BuildParameters, PqBuildParameters},
    pq::HnswQuantizer,
    Serializable,
};
use parallel_hnsw::{pq::QuantizedHnsw, progress::ProgressMonitor, SerializationError};
use parallel_hnsw::{Hnsw, VectorId};
use thiserror::Error;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use tokio_stream::wrappers::LinesStream;
use urlencoding::encode;

use crate::{
    comparator::{
        ArrayCentroidComparator, Centroid16Comparator, Centroid16Comparator1024,
        Centroid8Comparator, Disk1024Comparator, DiskOpenAIComparator, OpenAIComparator,
        Quantized16Comparator, Quantized16Comparator1024, Quantized8Comparator,
    },
    configuration::HnswConfiguration,
    domain::Domain,
    indexer::{create_index_name, index_serialization_path},
    openai::{embeddings_for, EmbeddingError, Model},
    server::Operation,
    vecmath::{
        Embedding, EuclideanDistance16For1024, CENTROID_16_LENGTH, CENTROID_8_LENGTH,
        EMBEDDING_LENGTH, EMBEDDING_LENGTH_1024, QUANTIZED_16_EMBEDDING_LENGTH,
        QUANTIZED_16_EMBEDDING_LENGTH_1024, QUANTIZED_8_EMBEDDING_LENGTH,
    },
    vectors::VectorStore,
};
use parallel_hnsw::pq::VectorSelector;

#[derive(Error, Debug)]
pub enum BatchError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    VectorizationError(#[from] VectorizationError),
    #[error(transparent)]
    IndexingError(#[from] IndexingError),
}

#[derive(Error, Debug)]
pub enum IndexingError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    SerializationError(#[from] SerializationError),
}

#[derive(Error, Debug)]
pub enum VectorizationError {
    #[error(transparent)]
    EmbeddingError(#[from] EmbeddingError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

async fn save_embeddings(
    vec_file: &mut File,
    offset: usize,
    embeddings: &[Embedding],
) -> Result<(), VectorizationError> {
    let transmuted = unsafe {
        std::slice::from_raw_parts(
            embeddings.as_ptr() as *const u8,
            std::mem::size_of_val(embeddings),
        )
    };
    vec_file
        .seek(SeekFrom::Start(
            (offset * std::mem::size_of::<Embedding>()) as u64,
        ))
        .await?;
    vec_file.write_all(transmuted).await?;
    vec_file.flush().await?;
    vec_file.sync_data().await?;

    Ok(())
}

pub async fn vectorize_from_operations<
    S: Stream<Item = io::Result<Operation>>,
    P: AsRef<Path> + Unpin,
>(
    api_key: &str,
    model: Model,
    vec_file: &mut File,
    op_stream: S,
    progress_file_path: P,
) -> Result<usize, VectorizationError> {
    let mut progress_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(progress_file_path)
        .await?;
    let mut offset;
    if progress_file.metadata().await?.size() != 8 {
        // assume we have to start from scratch
        progress_file.write_u64(0).await?;
        offset = 0;
    } else {
        offset = progress_file.read_u64().await?;
    }

    let filtered_op_stream = pin!(op_stream
        .try_filter(|o| future::ready(o.has_string()))
        .skip(offset as usize)
        .chunks(100));
    let mut taskstream = filtered_op_stream
        .map(|chunk| {
            let inner_api_key = api_key.to_string();
            tokio::spawn(async move { chunk_to_embeds(inner_api_key, chunk, model).await })
        })
        .buffered(10);

    let mut failures = 0;
    eprintln!("starting indexing at {offset}");
    while let Some(embeds) = taskstream.next().await {
        eprintln!("start of loop");
        let (embeddings, chunk_failures) = embeds.unwrap()?;
        eprintln!("retrieved embeddings");

        save_embeddings(vec_file, offset as usize, &embeddings).await?;
        eprintln!("saved embeddings");
        failures += chunk_failures;
        offset += embeddings.len() as u64;
        progress_file.seek(SeekFrom::Start(0)).await?;
        progress_file.write_u64(offset).await?;
        progress_file.flush().await?;
        progress_file.sync_data().await?;
        eprintln!("indexed {offset}");
    }

    Ok(failures)
}

async fn chunk_to_embeds(
    api_key: String,
    chunk: Vec<Result<Operation, io::Error>>,
    model: Model,
) -> Result<(Vec<Embedding>, usize), VectorizationError> {
    let chunk: Result<Vec<String>, _> = chunk
        .into_iter()
        .map(|o| o.map(|o| o.string().unwrap()))
        .collect();
    let chunk = chunk?;

    Ok(embeddings_for(&api_key, &chunk, model).await?)
}

async fn get_operations_from_file(
    file: &mut File,
) -> io::Result<impl Stream<Item = io::Result<Operation>> + '_> {
    file.seek(SeekFrom::Start(0)).await?;

    let buf_reader = BufReader::new(file);
    let lines = buf_reader.lines();
    let lines_stream = LinesStream::new(lines);
    let stream = lines_stream.and_then(|l| {
        future::ready(serde_json::from_str(&l).map_err(|e| io::Error::new(io::ErrorKind::Other, e)))
    });

    Ok(stream)
}

pub async fn extend_vector_store<P0: AsRef<Path>, P1: AsRef<Path>>(
    domain: &str,
    vectorlink_path: P0,
    vec_path: P1,
    size: usize,
    vector_size: usize,
) -> Result<usize, io::Error> {
    let vs_path: PathBuf = vectorlink_path.as_ref().into();
    let vs: VectorStore = VectorStore::new(vs_path, size);
    let domain = vs.get_domain_sized(domain, vector_size)?;
    Ok(domain.concatenate_file(&vec_path)?.0)
}

const NUMBER_OF_CENTROIDS: usize = 10_000;
pub async fn index_using_operations_and_vectors<
    P0: AsRef<Path>,
    P1: AsRef<Path>,
    P2: AsRef<Path>,
>(
    domain: &str,
    commit: &str,
    vectorlink_path: P0,
    staging_path: P1,
    op_file_path: P2,
    size: usize,
    id_offset: u64,
    quantize_hnsw: bool,
    model: Model,
    progress: &mut dyn ProgressMonitor,
) -> Result<(), IndexingError> {
    // Start at last hnsw offset
    let mut progress_file_path: PathBuf = staging_path.as_ref().into();
    progress_file_path.push("index_progress");

    let offset: u64;
    let mut progress_file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(progress_file_path)
        .await?;
    if progress_file.metadata().await?.size() != 8 {
        // assume we have to start from scratch
        progress_file.write_u64(id_offset).await?;
        offset = id_offset;
    } else {
        offset = progress_file.read_u64().await?;
    }

    // Start filling the HNSW
    let vs_path_buf: PathBuf = vectorlink_path.as_ref().into();
    let vs: VectorStore = VectorStore::new(&vs_path_buf, size);
    //    let index_id = create_index_name(domain, commit);
    let domain_obj = vs.get_domain_sized(domain, model.size())?;
    let mut op_file = File::open(&op_file_path).await?;
    let mut op_stream = get_operations_from_file(&mut op_file).await?;
    let mut i: usize = 0;

    let index_file_name = "index";
    //    let temp_file = index_serialization_path(&staging_path, temp_file_name);
    let staging_file = index_serialization_path(&staging_path, index_file_name);
    let index_name = create_index_name(domain, commit);
    let final_file = index_serialization_path(&vectorlink_path, &index_name);
    /*
    let mut hnsw: HnswIndex;
    if let Some(index) = deserialize_index(&staging_file, &domain_obj, &index_id, &vs)? {
        hnsw = index;
    } else {
        hnsw = deserialize_index(&final_file, &domain_obj, &index_id, &vs)?
            .unwrap_or_else(|| HnswIndex::new(OpenAI));
    }*/
    while let Some(op) = op_stream.next().await {
        match op.unwrap() {
            Operation::Inserted { .. } => i += 1,
            Operation::Changed { .. } => {
                todo!()
            }
            Operation::Deleted { .. } => {
                todo!()
            }
            Operation::Error { message } => {
                panic!("Error in indexing {message}");
            }
        }
    }
    assert_eq!(offset, 0);
    perform_indexing(
        domain_obj,
        offset,
        i,
        quantize_hnsw,
        model,
        staging_file,
        final_file,
        progress,
    )
}

fn perform_indexing(
    domain_obj: Arc<Domain>,
    _offset: u64,
    count: usize,
    quantize_hnsw: bool,
    model: Model,
    staging_file: PathBuf,
    final_file: PathBuf,
    progress: &mut dyn ProgressMonitor,
) -> Result<(), IndexingError> {
    progress.alive().unwrap();
    eprintln!("ready to generate hnsw");
    // NOTE: This should be a switch over the configurations
    // defined in HnswConfiguration
    match model {
        Model::Ada2 | Model::Small3 => {
            let hnsw = if quantize_hnsw {
                let number_of_centroids = 65_535;

                let comparator = DiskOpenAIComparator::new(
                    domain_obj.name().to_owned(),
                    Arc::new(domain_obj.immutable_file().into_sized()),
                );
                let pq_build_parameters = PqBuildParameters::default();

                let quantizer_path = staging_file.join("quantizer");
                let centroid_quantizer_result = keepalive!(
                    progress,
                    HnswQuantizer::<
                        EMBEDDING_LENGTH,
                        CENTROID_16_LENGTH,
                        QUANTIZED_16_EMBEDDING_LENGTH,
                        Centroid16Comparator,
                    >::deserialize(&quantizer_path, ())
                );
                let comparator_path = staging_file.join("hnsw/comparator");
                let deserialization_result =
                    centroid_quantizer_result.and_then(|centroid_quantizer| {
                        let quantized_comparator_result = keepalive!(
                            progress,
                            Quantized16Comparator::deserialize(
                                &comparator_path,
                                centroid_quantizer.comparator().clone(),
                            )
                        );
                        quantized_comparator_result
                            .map(|quantized_comparator| (centroid_quantizer, quantized_comparator))
                    });
                let (vids, centroid_quantizer, quantized_comparator) = match deserialization_result
                {
                    Ok((centroid_quantizer, quantized_comparator)) => (
                        (0..comparator.num_vecs()).map(VectorId).collect(),
                        centroid_quantizer,
                        quantized_comparator,
                    ),
                    _ => {
                        let (centroid_hnsw, quantized_comparator) = QuantizedHnsw::<
                            EMBEDDING_LENGTH,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH,
                            Centroid16Comparator,
                            Quantized16Comparator,
                            DiskOpenAIComparator,
                        >::generate_centroid_hnsw(
                            comparator.clone(),
                            number_of_centroids,
                            pq_build_parameters.centroids,
                            progress,
                        );

                        let centroid_quantizer: HnswQuantizer<
                            EMBEDDING_LENGTH,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH,
                            Centroid16Comparator,
                        > = HnswQuantizer::new(centroid_hnsw, pq_build_parameters);

                        let (vids, centroid_quantizer, quantized_comparator) = QuantizedHnsw::<
                            EMBEDDING_LENGTH,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH,
                            Centroid16Comparator,
                            Quantized16Comparator,
                            DiskOpenAIComparator,
                        >::perform_quantization(
                            comparator.clone(),
                            centroid_quantizer,
                            quantized_comparator,
                            progress,
                        );
                        keepalive!(progress, centroid_quantizer.serialize(quantizer_path))?;
                        keepalive!(progress, quantized_comparator.serialize(comparator_path))?;
                        (vids, centroid_quantizer, quantized_comparator)
                    }
                };
                let quantized_hnsw: QuantizedHnsw<
                    EMBEDDING_LENGTH,
                    CENTROID_16_LENGTH,
                    QUANTIZED_16_EMBEDDING_LENGTH,
                    Centroid16Comparator,
                    Quantized16Comparator,
                    DiskOpenAIComparator,
                > = QuantizedHnsw::new_with_quantized_vectors(
                    comparator,
                    pq_build_parameters,
                    vids,
                    centroid_quantizer,
                    quantized_comparator,
                    progress,
                );
                HnswConfiguration::SmallQuantizedOpenAi(model, quantized_hnsw)
            } else {
                panic!("No unquantized 1024 available");
            };
            eprintln!("done generating hnsw");
            keepalive!(progress, hnsw.serialize(&staging_file))?;
            eprintln!("done serializing hnsw");
            eprintln!("renaming {staging_file:?} to {final_file:?}");
            std::fs::rename(&staging_file, &final_file)?;
        }
        Model::MxBai => {
            let hnsw = if quantize_hnsw {
                let number_of_centroids = 65_535;

                let comparator = Disk1024Comparator::new(
                    domain_obj.name().to_owned(),
                    Arc::new(domain_obj.immutable_file().into_sized()),
                );
                let pq_build_parameters = PqBuildParameters::default();

                let quantizer_path = staging_file.join("quantizer");
                let centroid_quantizer_result = keepalive!(
                    progress,
                    HnswQuantizer::<
                        EMBEDDING_LENGTH_1024,
                        CENTROID_16_LENGTH,
                        QUANTIZED_16_EMBEDDING_LENGTH_1024,
                        Centroid16Comparator1024,
                    >::deserialize(&quantizer_path, ())
                );
                let comparator_path = staging_file.join("hnsw/comparator");
                let deserialization_result =
                    centroid_quantizer_result.and_then(|centroid_quantizer| {
                        let quantized_comparator_result = keepalive!(
                            progress,
                            Quantized16Comparator1024::deserialize(
                                &comparator_path,
                                centroid_quantizer.comparator().clone(),
                            )
                        );
                        quantized_comparator_result
                            .map(|quantized_comparator| (centroid_quantizer, quantized_comparator))
                    });
                let (vids, centroid_quantizer, quantized_comparator) = match deserialization_result
                {
                    Ok((centroid_quantizer, quantized_comparator)) => (
                        (0..comparator.num_vecs()).map(VectorId).collect(),
                        centroid_quantizer,
                        quantized_comparator,
                    ),
                    _ => {
                        let (centroid_hnsw, quantized_comparator) = QuantizedHnsw::<
                            EMBEDDING_LENGTH_1024,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH_1024,
                            Centroid16Comparator1024,
                            Quantized16Comparator1024,
                            Disk1024Comparator,
                        >::generate_centroid_hnsw(
                            comparator.clone(),
                            number_of_centroids,
                            pq_build_parameters.centroids,
                            progress,
                        );

                        let centroid_quantizer: HnswQuantizer<
                            EMBEDDING_LENGTH_1024,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH_1024,
                            Centroid16Comparator1024,
                        > = HnswQuantizer::new(centroid_hnsw, pq_build_parameters);

                        let (vids, centroid_quantizer, quantized_comparator) = QuantizedHnsw::<
                            EMBEDDING_LENGTH_1024,
                            CENTROID_16_LENGTH,
                            QUANTIZED_16_EMBEDDING_LENGTH_1024,
                            Centroid16Comparator1024,
                            Quantized16Comparator1024,
                            Disk1024Comparator,
                        >::perform_quantization(
                            comparator.clone(),
                            centroid_quantizer,
                            quantized_comparator,
                            progress,
                        );
                        keepalive!(progress, centroid_quantizer.serialize(quantizer_path))?;
                        keepalive!(progress, quantized_comparator.serialize(comparator_path))?;
                        (vids, centroid_quantizer, quantized_comparator)
                    }
                };
                let quantized_hnsw: QuantizedHnsw<
                    EMBEDDING_LENGTH_1024,
                    CENTROID_16_LENGTH,
                    QUANTIZED_16_EMBEDDING_LENGTH_1024,
                    Centroid16Comparator1024,
                    Quantized16Comparator1024,
                    Disk1024Comparator,
                > = QuantizedHnsw::new_with_quantized_vectors(
                    comparator,
                    pq_build_parameters,
                    vids,
                    centroid_quantizer,
                    quantized_comparator,
                    progress,
                );
                HnswConfiguration::Quantized1024By16(model, quantized_hnsw)
            } else {
                panic!("No unquantized 1024 available");
            };
            eprintln!("done generating hnsw");
            keepalive!(progress, hnsw.serialize(&staging_file))?;
            eprintln!("done serializing hnsw");
            eprintln!("renaming {staging_file:?} to {final_file:?}");
            std::fs::rename(&staging_file, &final_file)?;
        }
    };
    Ok(())
}

pub async fn index_from_operations_file<P: AsRef<Path>>(
    api_key: &str,
    model: Model,
    op_file_path: P,
    vectorlink_path: P,
    domain: &str,
    commit: &str,
    size: usize,
    build_index: bool,
    quantize_hnsw: bool,
    progress: &mut dyn ProgressMonitor,
) -> Result<(), BatchError> {
    let mut staging_path: PathBuf = vectorlink_path.as_ref().into();
    staging_path.push(".staging");
    staging_path.push(&*encode(domain));
    tokio::fs::create_dir_all(&staging_path).await?;

    let mut vector_path = staging_path.clone();
    vector_path.push("vectors");
    let mut vec_file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&vector_path)
        .await?;
    let mut progress_file_path = staging_path.clone();
    progress_file_path.push("progress");

    let mut op_file = File::open(&op_file_path).await?;
    let op_stream = get_operations_from_file(&mut op_file).await?;

    vectorize_from_operations(api_key, model, &mut vec_file, op_stream, progress_file_path).await?;

    // first append vectors in bulk
    let mut extended_path: PathBuf = staging_path.clone();
    extended_path.push("vectors_extended");
    let mut extended_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(extended_path)
        .await?;
    let id_offset: u64;
    if extended_file.metadata().await?.size() != 8 {
        eprintln!("Concatenating to vector store");
        id_offset = extend_vector_store(domain, &vectorlink_path, vector_path, size, model.size())
            .await? as u64;
        extended_file.write_u64(id_offset).await?;
    } else {
        eprintln!("Already concatenated");
        id_offset = extended_file.read_u64().await?;
    }

    if build_index {
        index_using_operations_and_vectors(
            domain,
            commit,
            vectorlink_path,
            staging_path,
            op_file_path,
            size,
            id_offset,
            quantize_hnsw,
            model,
            progress,
        )
        .await?;
    } else {
        eprintln!("No index built");
    }
    Ok(())
}

pub fn index_domain<P: AsRef<Path>>(
    _api_key: &str,
    model: Model,
    vectorlink_path: P,
    domain: &str,
    commit: &str,
    size: usize,
    quantize_hnsw: bool,
    progress: &mut (dyn ProgressMonitor + Send),
) -> Result<(), IndexingError> {
    let mut staging_path: PathBuf = vectorlink_path.as_ref().into();
    staging_path.push(".staging");
    staging_path.push(&*encode(domain));
    staging_path.push(&*encode(commit));
    std::fs::create_dir_all(&staging_path)?;

    let vs_path_buf: PathBuf = vectorlink_path.as_ref().into();
    let vs: VectorStore = VectorStore::new(vs_path_buf, size);

    let domain_obj = vs.get_domain_sized(domain, model.size())?;

    let index_name = create_index_name(domain, commit);

    let index_file_name = "index";
    let staging_file = index_serialization_path(&staging_path, index_file_name);

    let final_file = index_serialization_path(&vectorlink_path, &index_name);

    let vector_count = domain_obj.num_vecs();

    perform_indexing(
        domain_obj,
        0,
        vector_count,
        quantize_hnsw,
        model,
        staging_file,
        final_file,
        progress,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::vecmath::{normalize_vec, normalized_cosine_distance};
    use parallel_hnsw::{parameters::OptimizationParameters, pq::VectorSelector, Comparator};
    use rand::{distributions::Uniform, prelude::*};

    #[derive(Clone)]
    pub struct MemoryOpenAIComparator {
        domain: String,
        vectors: Arc<Vec<Embedding>>,
    }

    impl MemoryOpenAIComparator {
        pub fn new(domain: String, vectors: Arc<Vec<Embedding>>) -> Self {
            Self { domain, vectors }
        }
    }

    impl Comparator for MemoryOpenAIComparator {
        type T = Embedding;
        type Borrowable<'a> = &'a Embedding
        where Self: 'a;
        fn lookup(&self, v: VectorId) -> &Embedding {
            &self.vectors[v.0]
        }

        fn compare_raw(&self, v1: &Embedding, v2: &Embedding) -> f32 {
            normalized_cosine_distance(v1, v2)
        }
    }

    impl Serializable for MemoryOpenAIComparator {
        type Params = Arc<VectorStore>;
        fn serialize<P: AsRef<Path>>(&self, _path: P) -> Result<(), SerializationError> {
            todo!();
        }

        fn deserialize<P: AsRef<Path>>(
            _path: P,
            _store: Arc<VectorStore>,
        ) -> Result<Self, SerializationError> {
            todo!();
        }
    }

    impl VectorSelector for MemoryOpenAIComparator {
        type T = Embedding;

        fn selection(&self, size: usize) -> Vec<Self::T> {
            let num_vecs = self.vectors.len();
            if size as f32 >= 0.3 * num_vecs as f32 {
                let upper_bound = std::cmp::min(size, num_vecs);
                let mut result = (*self.vectors).clone();
                let mut rng = thread_rng();
                result.shuffle(&mut rng);
                result.truncate(upper_bound);

                return result.to_vec();
            }
            // we've deemed the size of the collection large enough to do
            // a repeated sampling on until we fill up our quota.
            let mut rng = thread_rng();
            let mut set = HashSet::new();
            let range = Uniform::from(0_usize..self.vectors.len());
            while set.len() != size {
                let candidate = rng.sample(range);
                set.insert(candidate);
            }

            set.into_iter().map(|index| self.vectors[index]).collect()
        }

        fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>> {
            let res = self.vectors.chunks(1_000_000).map(|x| x.to_vec());
            res
        }
    }

    // where do a put temp files for testing?
    #[test]
    fn test_comparator16_pq() {
        let vectors: Vec<Embedding> = (0..1_000_000)
            .map(|i| {
                let prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let range = Uniform::from(-1.0..1.0);
                let v: Vec<f32> = prng.sample_iter(&range).take(1536).collect();
                let mut buf = [0.0; 1536];
                buf.copy_from_slice(&v);
                normalize_vec(&mut buf);
                buf
            })
            .collect();

        let number_of_centroids = 65_535;
        let c = MemoryOpenAIComparator::new("my domain".to_string(), Arc::new(vectors));
        let pq_build_parameters = PqBuildParameters::default();
        let quantized_hnsw: QuantizedHnsw<
            EMBEDDING_LENGTH,
            CENTROID_16_LENGTH,
            QUANTIZED_16_EMBEDDING_LENGTH,
            Centroid16Comparator,
            Quantized16Comparator,
            MemoryOpenAIComparator,
        > = QuantizedHnsw::new(number_of_centroids, c, pq_build_parameters, &mut ());

        let op = OptimizationParameters::default();
        let res = quantized_hnsw.stochastic_recall(op);
        assert!(res > 0.97);
    }
}
