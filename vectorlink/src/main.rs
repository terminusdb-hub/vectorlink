#![feature(portable_simd)]
#![feature(trait_upcasting)]

use std::fs;
use std::io::stdout;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
mod batch;
mod comparator;
mod configuration;
mod domain;
mod indexer;
mod openai;
mod server;
mod vecmath;
mod vectors;
use parallel_hnsw::Comparator;

mod search_server;

use batch::index_from_operations_file;
use byteorder::LittleEndian;
use byteorder::WriteBytesExt;
use clap::CommandFactory;
use clap::{Parser, Subcommand, ValueEnum};
use configuration::HnswConfiguration;
//use hnsw::Hnsw;
use openai::Model;
use parallel_hnsw::parameters::OptimizationParameters;
use parallel_hnsw::parameters::SearchParameters;
use parallel_hnsw::pq::QuantizationStatistics;
use parallel_hnsw::progress::SimpleProgressMonitor;
use parallel_hnsw::AbstractVector;
use parallel_hnsw::Serializable;
use parallel_hnsw::VectorId;
use rand::prelude::*;
use std::fs::File;
use std::io;
use vecmath::Embedding1024;
use vecmath::EMBEDDING_BYTE_LENGTH;
use vecmath::EMBEDDING_LENGTH;

use rayon::iter::Either;
use rayon::prelude::*;

use crate::batch::index_domain;
use crate::comparator::Disk1024Comparator;
use crate::search_server::MatchResult;
use crate::vecmath::normalize_vec;
use crate::vecmath::Embedding;
use crate::vecmath::EMBEDDING_BYTE_LENGTH_1024;

use {indexer::create_index_name, vecmath::empty_embedding, vectors::VectorStore};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SearchOutputFormat {
    TwoColumn,
    Json,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Serve {
        #[arg(short, long)]
        content_endpoint: Option<String>,
        #[arg(short, long)]
        user_forward_header: Option<String>,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
    },
    Load {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long)]
        input: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, value_enum, default_value_t = Model::Ada2)]
        model: Model,
        #[arg(long)]
        build_index: Option<bool>,
        #[arg(short, long)]
        quantize_hnsw: bool,
    },
    Index {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, value_enum, default_value_t = Model::Ada2)]
        model: Model,
        #[arg(short, long)]
        quantize_hnsw: bool,
    },
    Embed {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(short, long)]
        string: String,
        #[arg(short, long, value_enum, default_value_t = Model::Ada2)]
        model: Model,
        #[arg(short, long)]
        raw: bool,
    },
    CompareQuantized {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(long)]
        v1: usize,
        #[arg(long)]
        v2: usize,
    },
    Compare2 {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(long)]
        s1: String,
        #[arg(long)]
        s2: String,
        #[arg(short, long, value_enum, default_value_t = DistanceVariant::Default)]
        variant: DistanceVariant,
        #[arg(short, long, value_enum, default_value_t = Model::Ada2)]
        model: Model,
    },
    CompareModels {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(long)]
        word: String,
        #[arg(long)]
        near1: String,
        #[arg(long)]
        near2: String,
    },
    CompareRaw {},
    TestRecall {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, default_value_t = 0.99)]
        recall_confidence: f32,
    },
    Duplicates {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long)]
        take: Option<usize>,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, default_value_t = 1.0_f32)]
        threshold: f32,
        #[arg(short, long, value_enum, default_value_t = SearchOutputFormat::TwoColumn)]
        output_format: SearchOutputFormat,
    },
    Test {
        #[arg(short, long)]
        key: Option<String>,
        #[arg(short, long, value_enum, default_value_t = Model::Ada2)]
        model: Model,
    },
    ImproveIndex {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short = 't', long, default_value_t = 1.0)]
        promotion_threshold: f32,
        #[arg(short, long, default_value_t = 0.01)]
        neighbor_threshold: f32,
        #[arg(short, long, default_value_t = 0.99)]
        recall_confidence: f32,
        #[arg(short, long, default_value_t = 1.0)]
        promotion_proportion: f32,
    },
    ImproveNeighbors {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, default_value_t = 0.01)]
        threshold: f32,
        #[arg(short, long, default_value_t = 0.99)]
        recall_confidence: f32,
    },
    PromoteAtLayer {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, default_value_t)]
        layer: usize,
        #[arg(short, long, default_value_t = 0.05)]
        max_proportion: f32,
    },
    ScanNeighbors {
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(long)]
        sequence_domain: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long, default_value_t = 1.0_f32)]
        threshold: f32,
    },
    TestQuantization {
        #[arg(short, long)]
        directory: String,
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
    },
    Search {
        #[arg(short, long)]
        directory: String,
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long)]
        vid: Option<usize>,
    },
    SearchServer {
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
        #[arg(short, long)]
        operations_file: String,
        #[arg(short, long)]
        directory: String,
        #[arg(short, long)]
        commit: String,
        #[arg(long)]
        domain: String,
        #[arg(short, long, default_value_t = 10000)]
        size: usize,
        #[arg(short, long)]
        key: Option<String>,
    },
    Scramble {
        #[arg(short, long)]
        vec_file: String,
        #[arg(short, long)]
        output_vecs: String,
        #[arg(short, long)]
        output_map: String,
        #[arg(short, long)]
        vector_size: usize,
    },
    ScaleVecs {
        source_vector_file: String,
        target_vector_file: String,
        #[arg(short, long)]
        source_vector_size: usize,
        #[arg(short, long)]
        target_vector_size: usize,
    },
    Normalize {
        source_vector_file: String,
        target_vector_file: String,
        #[arg(short, long)]
        vector_size: usize,
    },

    FanoutVecs {
        source_vector_file: String,
        target_vector_dir: String,
        #[arg(short, long)]
        vector_size: usize,
        #[arg(short = 'c', long)]
        selection_count: usize,
        #[arg(short, long)]
        single_selection_proportion: f32,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DistanceVariant {
    Default,
    Simd,
    Scalar,
}

fn key_or_env(k: Option<String>) -> String {
    let result = k.or_else(|| std::env::var("OPENAI_KEY").ok());
    if result.is_none() {
        let mut app = Args::command();
        eprintln!("Error: no OpenAI key given. Configure it with the OPENAI_KEY environment variable, or by passing in the --key argument");
        app.print_help().unwrap();
        std::process::exit(2);
    }

    result.unwrap()
}

fn content_endpoint_or_env(c: Option<String>) -> Option<String> {
    c.or_else(|| std::env::var("TERMINUSDB_CONTENT_ENDPOINT").ok())
}

fn user_forward_header_or_env(c: Option<String>) -> String {
    c.unwrap_or_else(|| std::env::var("TERMINUSDB_USER_FORWARD_HEADER").unwrap())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    match args.command {
        Commands::Serve {
            content_endpoint,
            user_forward_header,
            directory,
            port,
            size,
        } => {
            server::serve(
                directory,
                user_forward_header_or_env(user_forward_header),
                port,
                size,
                content_endpoint_or_env(content_endpoint),
            )
            .await?
        }
        Commands::Embed {
            key,
            string,
            model,
            raw,
        } => {
            let v: Vec<[f32; 1536]> = openai::embeddings_for(&key_or_env(key), &[string], model)
                .await?
                .0;
            if raw {
                let ptr = v.as_ptr() as *const u8;
                let buf = unsafe {
                    std::slice::from_raw_parts(ptr, v.len() * std::mem::size_of::<[f32; 1536]>())
                };
                eprintln!("going to write {} bytes", buf.len());
                let mut stdout = stdout();
                stdout.write_all(buf).unwrap();
                stdout.flush().unwrap();
            } else {
                eprintln!("{:?}", v);
            }
        }
        Commands::CompareRaw {} => {
            let mut bufs: [Embedding1024; 2] =
                unsafe { std::mem::transmute([0_u8; EMBEDDING_BYTE_LENGTH_1024 * 2]) };
            let mut stdin = std::io::stdin().lock();
            unsafe {
                let slice = std::slice::from_raw_parts_mut(
                    bufs.as_ptr() as *mut u8,
                    EMBEDDING_BYTE_LENGTH_1024,
                );
                stdin.read_exact(slice).unwrap();
            }

            loop {
                let result = unsafe {
                    let slice = std::slice::from_raw_parts_mut(
                        bufs[1..].as_ptr() as *mut u8,
                        EMBEDDING_BYTE_LENGTH_1024,
                    );

                    stdin.read_exact(slice)
                };

                if let Some(err) = result.as_ref().err() {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        // dont care
                        break;
                    }

                    // do care
                }

                result.unwrap();

                let distance = vecmath::normalized_cosine_distance_1024(&bufs[0], &bufs[1]);
                println!("{distance}");
            }
        }
        Commands::CompareQuantized {
            v1,
            v2,
            commit,
            domain,
            directory,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, 1234);
            let hnsw = HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap();
            let res = match hnsw {
                HnswConfiguration::QuantizedOpenAi(_, _) => todo!(),
                HnswConfiguration::SmallQuantizedOpenAi(_, _) => todo!(),
                HnswConfiguration::SmallQuantizedOpenAi8(_, _) => todo!(),
                HnswConfiguration::SmallQuantizedOpenAi4(_, _) => todo!(),
                HnswConfiguration::UnquantizedOpenAi(_, _) => todo!(),
                HnswConfiguration::Quantized1024By16(_, q) => q.compare(v1, v2),
            };
            eprintln!("result: {res:?}");
        }
        Commands::Compare2 {
            key,
            s1,
            s2,
            variant,
            model,
        } => {
            let v = openai::embeddings_for(&key_or_env(key), &[s1, s2], model)
                .await?
                .0;
            let p1 = &v[0];
            let p2 = &v[1];
            let distance = match variant {
                DistanceVariant::Default => vecmath::normalized_cosine_distance(p1, p2),
                DistanceVariant::Scalar => vecmath::normalized_cosine_distance_scalar(p1, p2),
                DistanceVariant::Simd => vecmath::normalized_cosine_distance_simd(p1, p2),
            };
            println!("distance: {}", distance);
        }
        Commands::CompareModels {
            key,
            word,
            near1,
            near2,
        } => {
            let strings = [word, near1, near2];
            for model in [Model::Ada2, Model::Small3] {
                let v = openai::embeddings_for(&key_or_env(key.clone()), &strings, model)
                    .await?
                    .0;
                let embedding_word = &v[0];
                let embedding_n1 = &v[1];
                let embedding_n2 = &v[2];
                let distance1 = vecmath::normalized_cosine_distance(embedding_word, embedding_n1);
                let distance2 = vecmath::normalized_cosine_distance(embedding_word, embedding_n2);
                println!("{model:?}: {distance1} {distance2}");
            }
        }
        Commands::Test { key, model } => {
            let v = openai::embeddings_for(
                &key_or_env(key),
                &[
                    "king".to_string(),
                    "man".to_string(),
                    "woman".to_string(),
                    "queen".to_string(),
                ],
                model,
            )
            .await?
            .0;
            let mut calculated = empty_embedding();
            for (i, calculated) in calculated.iter_mut().enumerate() {
                *calculated = v[0][i] - v[1][i] + v[2][i];
            }
            let distance = vecmath::normalized_cosine_distance(&v[3], &calculated);
            eprintln!("{}", distance);
        }
        Commands::Load {
            key,
            domain,
            directory,
            input,
            size,
            commit,
            model,
            build_index,
            quantize_hnsw,
        } => {
            eprintln!("starting load");
            let key = key_or_env(key);
            index_from_operations_file(
                &key,
                model,
                input,
                directory,
                &domain,
                &commit,
                size,
                build_index.unwrap_or(true),
                quantize_hnsw,
                &mut SimpleProgressMonitor::default(),
            )
            .await
            .unwrap()
        }
        Commands::Index {
            key,
            domain,
            model,
            directory,
            size,
            commit,
            quantize_hnsw,
        } => {
            eprintln!("starting indexing");
            let key = key_or_env(key);
            index_domain(
                &key,
                model,
                directory,
                &domain,
                &commit,
                size,
                quantize_hnsw,
                &mut SimpleProgressMonitor::default(),
            )
            .unwrap()
        }
        Commands::TestRecall {
            domain,
            directory,
            size,
            commit,
            recall_confidence,
        } => {
            eprintln!("Testing recall");
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);
            let hnsw = HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap();
            let mut optimization_parameters = OptimizationParameters::default();
            optimization_parameters.recall_confidence = recall_confidence;
            let recall = hnsw.stochastic_recall(optimization_parameters);
            eprintln!("Recall: {recall}");
        }
        Commands::Duplicates {
            commit,
            domain,
            size,
            take,
            directory,
            threshold,
            output_format,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);
            let hnsw = HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap();

            let sp = SearchParameters::default();
            let elts = if let Some(take) = take {
                Either::Left(hnsw.threshold_nn(threshold, sp).take_any(take))
            } else {
                Either::Right(hnsw.threshold_nn(threshold, sp))
            };
            let stdout = std::io::stdout();
            match output_format {
                SearchOutputFormat::TwoColumn => {
                    elts.for_each(move |(v, results)| {
                        let mut cluster = Vec::new();
                        for result in results.iter() {
                            let distance = result.1;
                            if distance < threshold {
                                cluster.push((result.0 .0, distance));
                                let mut lock = stdout.lock();
                                lock.write_u64::<LittleEndian>(v.0 as u64).unwrap();
                                lock.write_u64::<LittleEndian>(result.0 .0 as u64).unwrap();
                            }
                        }
                    });
                }
                SearchOutputFormat::Json => {
                    elts.for_each(|(v, results)| {
                        let mut cluster = Vec::new();
                        for result in results.iter() {
                            let distance = result.1;
                            if distance < threshold {
                                cluster.push((result.0 .0, distance))
                            }
                        }
                        if !cluster.is_empty() {
                            let cluster = serde_json::to_string(&cluster).unwrap();
                            let mut lock = stdout.lock();
                            writeln!(lock, "[{}, {}]", v.0, cluster).unwrap();
                        }
                    });
                }
            }
        }
        Commands::ImproveIndex {
            commit,
            domain,
            directory,
            size,
            promotion_threshold,
            neighbor_threshold,
            recall_confidence,
            promotion_proportion,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);

            let mut hnsw: HnswConfiguration =
                HnswConfiguration::deserialize(&hnsw_index_path, Arc::new(store)).unwrap();
            let mut build_parameters = hnsw.build_parameters_for_improve_index();
            build_parameters.optimization.promotion_threshold = promotion_threshold;
            build_parameters.optimization.promotion_proportion = promotion_proportion;
            build_parameters.optimization.neighborhood_threshold = neighbor_threshold;
            build_parameters.optimization.recall_confidence = recall_confidence;
            hnsw.improve_index(build_parameters, &mut SimpleProgressMonitor::default());

            // TODO should write to staging first
            hnsw.serialize(hnsw_index_path)?;
        }

        Commands::ImproveNeighbors {
            commit,
            domain,
            directory,
            size,
            threshold,
            recall_confidence,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);

            let mut hnsw: HnswConfiguration =
                HnswConfiguration::deserialize(&hnsw_index_path, Arc::new(store)).unwrap();

            let mut bp = hnsw.build_parameters_for_improve_index();
            bp.optimization.neighborhood_threshold = threshold;
            bp.optimization.recall_confidence = recall_confidence;

            // TODO do a quick test recall here
            hnsw.improve_neighbors(bp.optimization, None);

            // TODO should write to staging first
            hnsw.serialize(hnsw_index_path)?;
        }
        Commands::PromoteAtLayer {
            commit,
            domain,
            directory,
            size,
            layer,
            max_proportion,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);

            let mut hnsw: HnswConfiguration =
                HnswConfiguration::deserialize(&hnsw_index_path, Arc::new(store)).unwrap();
            let mut bp = hnsw.build_parameters_for_improve_index();
            bp.optimization.promotion_proportion = max_proportion;
            if hnsw.promote_at_layer(layer, bp) {
                eprintln!("promoted nodes at layer {layer}");
                // TODO should write to staging first
                hnsw.serialize(hnsw_index_path)?;
            }
        }
        Commands::ScanNeighbors {
            commit,
            domain,
            sequence_domain,
            directory,
            size,
            threshold,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);
            let hnsw = HnswConfiguration::deserialize(&hnsw_index_path, Arc::new(store)).unwrap();

            let mut sequence_path = PathBuf::from(directory);
            sequence_path.push(format!("{sequence_domain}.vecs"));
            let mut embedding = [0; EMBEDDING_BYTE_LENGTH];
            let mut sequence_file = File::open(sequence_path).unwrap();
            let mut sequence_index = 0; // todo file offsetting etc
            let output = std::io::stdout();
            loop {
                match sequence_file.read_exact(&mut embedding) {
                    Ok(()) => {
                        let converted_embedding: &[f32; EMBEDDING_LENGTH] =
                            unsafe { std::mem::transmute(&embedding) };
                        let bp = hnsw.build_parameters_for_improve_index();
                        let sp = bp.optimization.search;
                        let search_result: Vec<_> = hnsw
                            .search(AbstractVector::Unstored(converted_embedding), sp)
                            .into_iter()
                            .filter(|r| r.1 < threshold)
                            .map(|r| (r.0 .0, r.1))
                            .collect();
                        let result_tuple = (sequence_index, search_result);
                        {
                            let mut lock = output.lock();
                            serde_json::to_writer(&mut lock, &result_tuple).unwrap();
                            writeln!(&mut lock).unwrap();
                        }

                        // do index lookup stuff
                        sequence_index += 1;
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => {
                        panic!("error occured while processing sequence vector file: {}", e);
                    }
                }
            }
        }
        Commands::TestQuantization {
            commit,
            domain,
            directory,
            size,
        } => {
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, size);
            let hnsw = HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap();
            match hnsw.quantization_statistics() {
                Some(qs) => {
                    let QuantizationStatistics {
                        sample_avg,
                        sample_var,
                        sample_deviation,
                    } = qs;
                    eprintln!("sample avg: {sample_avg}\nsample var: {sample_var}\nsample deviation: {sample_deviation}");
                }
                None => panic!("not a quantized hnsw"),
            }
        }
        Commands::SearchServer {
            port,
            operations_file,
            directory,
            commit,
            domain,
            size,
            key,
        } => {
            let key = key_or_env(key);
            search_server::serve(
                port,
                &operations_file,
                &directory,
                &commit,
                &domain,
                size,
                &key,
            )
            .await
            .unwrap()
        }
        Commands::Search {
            directory,
            commit,
            domain,
            vid,
        } => {
            // maybe send in search parameters
            let dirpath = Path::new(&directory);
            let hnsw_index_path = dbg!(format!(
                "{}/{}.hnsw",
                directory,
                create_index_name(&domain, &commit)
            ));
            let store = VectorStore::new(dirpath, 1234);
            let hnsw = HnswConfiguration::deserialize(hnsw_index_path, Arc::new(store)).unwrap();
            eprintln!("we deserialized an hnsw");
            let vector_size = hnsw.vector_size();
            let vector_byte_size = std::mem::size_of::<f32>() * vector_size;
            eprintln!("got vector size {vector_size}");
            let mut vector = vec![0.0_f32; vector_size];

            let abstract_vector = if let Some(vid) = vid {
                match &hnsw {
                    HnswConfiguration::Quantized1024By16(_, q) => {
                        let fc: Disk1024Comparator = q.full_comparator().clone();
                        let vec = fc.lookup(VectorId(vid));
                        vector[0..vector_size].clone_from_slice(&*vec);
                        AbstractVector::Unstored(&vector)
                    }
                    HnswConfiguration::UnquantizedOpenAi(_, h) => {
                        eprintln!("looking up vector by id");
                        let c = h.comparator();
                        let vec = c.lookup(VectorId(vid));
                        vector[0..vector_size].clone_from_slice(&*vec);
                        AbstractVector::Unstored(&vector)
                    }
                    _ => panic!("oops"),
                }
            } else {
                let mut stdin = std::io::stdin();
                eprintln!("reading {vector_byte_size} bytes");
                let mut buf = vec![0_u8; vector_byte_size];
                stdin.read_exact(&mut buf).unwrap();
                let slice =
                    unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const f32, vector_size) };
                vector[0..vector_size].clone_from_slice(slice);
                AbstractVector::Unstored(&vector)
            };
            let mut sp = SearchParameters::default();
            sp.number_of_candidates = 300;
            let results: Vec<MatchResult> = match vector_size {
                1024 => {
                    let vec: AbstractVector<[f32; 1024]> = abstract_vector.convert_to_array();
                    hnsw.search_1024(vec, sp)
                        .into_iter()
                        .map(|x| MatchResult {
                            id: x.0 .0.to_string(),
                            distance: x.1,
                        })
                        .collect()
                }
                1536 => {
                    let vec: AbstractVector<[f32; 1536]> = abstract_vector.convert_to_array();
                    hnsw.search(vec, sp)
                        .into_iter()
                        .map(|x| MatchResult {
                            id: x.0 .0.to_string(),
                            distance: x.1,
                        })
                        .collect()
                }
                _ => panic!("unsupported size"),
            };

            let stdout = std::io::stdout();
            let json = serde_json::to_string(&results).unwrap();
            let mut lock = stdout.lock();
            writeln!(lock, "{}", json).unwrap();
        }
        Commands::Scramble {
            vec_file,
            output_vecs,
            output_map,
            vector_size,
        } => {
            let vec_file = File::open(vec_file).unwrap();
            let mut output_vecs = File::create(output_vecs).unwrap();

            let byte_size = vec_file.metadata().unwrap().size() as usize;
            assert!(byte_size % vector_size == 0);
            let vector_byte_size = vector_size * std::mem::size_of::<f32>();
            let number_of_vecs = byte_size / vector_byte_size;

            let mut remap: Vec<usize> = (0..number_of_vecs).collect();
            remap.shuffle(&mut thread_rng());

            let remap_buf = unsafe {
                std::slice::from_raw_parts(
                    remap.as_ptr() as *const u8,
                    number_of_vecs * std::mem::size_of::<usize>(),
                )
            };

            std::fs::write(output_map, remap_buf).unwrap();

            let mut buf = vec![0; vector_byte_size];
            for (current, mapping) in remap.iter().enumerate() {
                if 100 * current % number_of_vecs == 0 {
                    eprintln!("scrambing {}%", 100 * current / number_of_vecs);
                }
                let byte_offset = mapping * vector_size;
                vec_file.read_at(&mut buf, byte_offset as u64).unwrap();
                output_vecs.write_all(&buf).unwrap();
            }
        }
        Commands::ScaleVecs {
            source_vector_file,
            target_vector_file,
            source_vector_size,
            target_vector_size,
        } => {
            assert!(source_vector_size < target_vector_size);
            let mut source_vector_file = File::open(source_vector_file).unwrap();
            let mut target_vector_file = File::create(target_vector_file).unwrap();

            let source_byte_size = source_vector_file.metadata().unwrap().size() as usize;
            assert!(source_byte_size % source_vector_size == 0);
            let source_vector_byte_size = source_vector_size * std::mem::size_of::<f32>();
            let number_of_vecs = source_byte_size / source_vector_byte_size;

            let mut vec = vec![0.0f32; target_vector_size];

            for i in 0..number_of_vecs {
                if i % 100_000 == 0 {
                    eprintln!("{i}/{number_of_vecs}");
                }
                let buf = unsafe {
                    std::slice::from_raw_parts_mut(
                        vec.as_mut_ptr() as *mut u8,
                        source_vector_size * std::mem::size_of::<f32>(),
                    )
                };

                source_vector_file.read_exact(buf).unwrap();
                let buf = unsafe {
                    std::slice::from_raw_parts(
                        vec.as_ptr() as *mut u8,
                        target_vector_size * std::mem::size_of::<f32>(),
                    )
                };
                target_vector_file.write_all(buf).unwrap();
            }

            target_vector_file.flush().unwrap();
        }
        Commands::Normalize {
            source_vector_file,
            target_vector_file,
            vector_size,
        } => {
            let mut source_vector_file = File::open(source_vector_file).unwrap();
            let mut target_vector_file = File::create(target_vector_file).unwrap();

            let vector_byte_size = source_vector_file.metadata().unwrap().size() as usize;
            assert!(vector_byte_size % vector_size == 0);
            assert!(vector_size == 1536);
            let source_vector_byte_size = vector_size * std::mem::size_of::<f32>();
            let number_of_vecs = vector_byte_size / source_vector_byte_size;

            let mut vec = vec![0.0f32; vector_size];

            for i in 0..number_of_vecs {
                if i % 100_000 == 0 {
                    eprintln!("{i}/{number_of_vecs}");
                }
                let buf = unsafe {
                    std::slice::from_raw_parts_mut(
                        vec.as_mut_ptr() as *mut u8,
                        vector_size * std::mem::size_of::<f32>(),
                    )
                };
                source_vector_file.read_exact(buf).unwrap();

                let embedding: &mut Embedding =
                    unsafe { &mut *(vec.as_mut_ptr() as *mut Embedding) };
                normalize_vec(embedding);

                let buf = unsafe {
                    std::slice::from_raw_parts(
                        vec.as_ptr() as *mut u8,
                        vector_size * std::mem::size_of::<f32>(),
                    )
                };
                target_vector_file.write_all(buf).unwrap();
            }

            target_vector_file.flush().unwrap();
        }
        Commands::FanoutVecs {
            source_vector_file,
            target_vector_dir,
            selection_count,
            single_selection_proportion,
            vector_size,
        } => {
            assert!(single_selection_proportion <= 1.0);
            // figure out vector count
            let source_vector_file_size =
                fs::metadata(&source_vector_file).unwrap().size() as usize;
            let vector_byte_size = std::mem::size_of::<f32>() * vector_size;
            let vector_count = source_vector_file_size / vector_byte_size;
            assert_eq!(0, source_vector_file_size % vector_byte_size);

            let single_selection_size =
                (single_selection_proportion * vector_count as f32).ceil() as usize;

            let target_path: PathBuf = target_vector_dir.into();
            fs::create_dir_all(&target_path).unwrap();

            // assume scramble sizes are large enough to warrant full
            // reordering rather than random extraction.
            let mut rng = thread_rng();
            for i in 0..selection_count {
                let mut indexes: Vec<usize> = (0..vector_count).collect();
                indexes.shuffle(&mut rng);
                indexes.truncate(single_selection_size);

                // resort so we can do log searches
                indexes.sort();

                let buf = unsafe {
                    std::slice::from_raw_parts(
                        indexes.as_ptr() as *mut u8,
                        vector_size * std::mem::size_of::<f32>(),
                    )
                };

                fs::write(target_path.join(format!("{i}.map")), buf).unwrap();

                let input_vec_file = File::open(&source_vector_file).unwrap();
                let mut output_vec_file =
                    File::create(target_path.join(format!("{i}.vecs"))).unwrap();

                let mut buf = vec![0_u8; vector_byte_size];
                for i in indexes {
                    let offset = i * vector_byte_size;
                    input_vec_file
                        .read_exact_at(&mut buf, offset as u64)
                        .unwrap();

                    output_vec_file.write_all(&buf).unwrap();
                }

                output_vec_file.flush().unwrap();
            }
        }
    };

    Ok(())
}
