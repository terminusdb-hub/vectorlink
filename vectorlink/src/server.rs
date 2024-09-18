#![allow(unused, dead_code)]
use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use futures::StreamExt;
use futures::TryStreamExt;

use hyper::HeaderMap;
use hyper::StatusCode;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, Uri,
};
use lazy_static::lazy_static;
use parallel_hnsw::AbstractVector;
use parallel_hnsw::AllVectorIterator;
use parallel_hnsw::Hnsw;
use parallel_hnsw::SerializationError;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rayon::prelude::*;
use regex::Regex;
use serde::Serialize;
use serde::{self, Deserialize};
use serde_json::json;
use std::collections::HashSet;
use std::string;
use std::sync::atomic::AtomicUsize;
use std::{
    collections::HashMap,
    convert::Infallible,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    slice::Iter,
    sync::Arc,
};
use std::{
    future,
    io::{self, ErrorKind},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task;
use tokio::task::JoinError;
use tokio::{io::AsyncBufReadExt, sync::RwLock};
use tokio_stream::{wrappers::LinesStream, Stream};
use tokio_util::io::StreamReader;

use crate::configuration::OpenAIHnsw;
use crate::indexer::create_index_name;
use crate::indexer::index_serialization_path;
use crate::indexer::search;
use crate::indexer::IndexError;
use crate::indexer::Point;
use crate::indexer::PointOperation;
use crate::indexer::SearchError;
use crate::indexer::{start_indexing_from_operations, IndexIdentifier, OpenAI};
use crate::openai::Model;
use crate::openai::{embeddings_for, EmbeddingError};
use crate::vectors::VectorStore;

#[derive(Clone, Deserialize, Debug)]
#[serde(tag = "op")]
pub enum Operation {
    Inserted { string: String, id: String },
    Changed { string: String, id: String },
    Deleted { id: String },
    Error { message: String },
}

impl Operation {
    pub fn has_string(&self) -> bool {
        matches!(self, Operation::Inserted { .. } | Operation::Changed { .. })
    }

    pub fn string(self) -> Option<String> {
        match self {
            Operation::Inserted { string, id } => Some(string),
            Operation::Changed { string, id } => Some(string),
            Operation::Deleted { id } => None,
            Operation::Error { message } => None,
        }
    }

    pub fn id(self) -> Option<String> {
        match self {
            Operation::Inserted { id, .. }
            | Operation::Changed { id, .. }
            | Operation::Deleted { id } => Some(id),
            Operation::Error { .. } => None,
        }
    }
}

#[derive(Deserialize, Debug)]
struct IndexRequest {
    domain: String,
    commit: String,
    previous: Option<String>,
    operations: Vec<Operation>,
}

#[derive(Debug)]
enum ResourceSpec {
    Search {
        domain: String,
        commit: String,
        count: usize,
    },
    StartIndex {
        domain: String,
        commit: String,
        previous: Option<String>,
    },
    AssignIndex {
        domain: String,
        source_commit: String,
        target_commit: String,
    },
    CheckTask {
        task_id: String,
    },
    Similar {
        domain: String,
        commit: String,
        id: String,
        count: usize,
    },
    DuplicateCandidates {
        domain: String,
        commit: String,
        threshold: Option<f32>,
        candidates: Option<usize>,
    },
    GetStatistics,
}

#[derive(Debug, Error)]
enum SpecParseError {
    #[error("Unknown URL Path")]
    UnknownPath,
    #[error("No task id")]
    NoTaskId,
    #[error("No commit id or domain id given")]
    NoCommitIdOrDomain,
}

pub(crate) fn query_map(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_else(|| HashMap::with_capacity(0))
}

#[derive(Debug, Error)]
enum HeaderError {
    #[error("Key was not valid utf8")]
    KeyNotUtf8,
    #[error("Missing the key {0}")]
    MissingKey(String),
}

fn get_header_value(header: &HeaderMap, key: &str) -> Result<String, HeaderError> {
    let value = header.get(key);
    match value {
        Some(value) => {
            let value = String::from_utf8(value.as_bytes().to_vec());
            match value {
                Ok(value) => Ok(value),
                Err(_) => Err(HeaderError::KeyNotUtf8),
            }
        }
        None => Err(HeaderError::MissingKey(key.to_string())),
    }
}

fn uri_to_spec(uri: &Uri) -> Result<ResourceSpec, SpecParseError> {
    lazy_static! {
        static ref RE_INDEX: Regex = Regex::new(r"^/index(/?)$").unwrap();
        static ref RE_ASSIGN: Regex = Regex::new(r"^/assign(/?)$").unwrap();
        static ref RE_CHECK: Regex = Regex::new(r"^/check(/?)$").unwrap();
        static ref RE_SEARCH: Regex = Regex::new(r"^/search(/?)$").unwrap();
        static ref RE_SIMILAR: Regex = Regex::new(r"^/similar(/?)$").unwrap();
        static ref RE_DUPLICATES: Regex = Regex::new(r"^/duplicates(/?)$").unwrap();
        static ref RE_STATISTICS: Regex = Regex::new(r"^/statistics$").unwrap();
    }
    let path = uri.path();

    if RE_INDEX.is_match(path) {
        let query = dbg!(query_map(uri));
        let commit = query.get("commit").map(|v| v.to_string());
        let domain = query.get("domain").map(|v| v.to_string());
        let previous = query.get("previous").map(|v| v.to_string());
        match (domain, commit) {
            (Some(domain), Some(commit)) => Ok(ResourceSpec::StartIndex {
                domain,
                commit,
                previous,
            }),
            _ => Err(SpecParseError::NoCommitIdOrDomain),
        }
    } else if RE_ASSIGN.is_match(path) {
        let query = query_map(uri);
        let domain = query.get("domain").map(|v| v.to_string());
        let source_commit = query.get("source_commit").map(|v| v.to_string());
        let target_commit = query.get("target_commit").map(|v| v.to_string());
        match (domain, source_commit, target_commit) {
            (Some(domain), Some(source_commit), Some(target_commit)) => {
                Ok(ResourceSpec::AssignIndex {
                    domain,
                    source_commit,
                    target_commit,
                })
            }
            _ => Err(SpecParseError::NoCommitIdOrDomain),
        }
    } else if RE_CHECK.is_match(path) {
        let query = query_map(uri);
        if let Some(task_id) = query.get("task_id") {
            Ok(ResourceSpec::CheckTask {
                task_id: task_id.to_string(),
            })
        } else {
            Err(SpecParseError::NoTaskId)
        }
    } else if RE_SEARCH.is_match(path) {
        let query = query_map(uri);
        let domain = query.get("domain").map(|v| v.to_string());
        let commit = query.get("commit").map(|v| v.to_string());
        let count = query.get("count").map(|v| v.parse::<usize>().unwrap());
        match (domain, commit) {
            (Some(domain), Some(commit)) => {
                let count = count.unwrap_or(10);
                Ok(ResourceSpec::Search {
                    domain,
                    commit,
                    count,
                })
            }
            _ => Err(SpecParseError::NoCommitIdOrDomain),
        }
    } else if RE_SIMILAR.is_match(path) {
        let query = query_map(uri);
        let domain = query.get("domain").map(|v| v.to_string());
        let commit = query.get("commit").map(|v| v.to_string());
        let id = query.get("id").map(|v| v.to_string());
        let count = query.get("count").map(|v| v.parse::<usize>().unwrap());
        match (domain, commit, id) {
            (Some(domain), Some(commit), Some(id)) => {
                let count = count.unwrap_or(10);
                Ok(ResourceSpec::Similar {
                    domain,
                    commit,
                    id,
                    count,
                })
            }
            _ => Err(SpecParseError::NoCommitIdOrDomain),
        }
    } else if RE_DUPLICATES.is_match(path) {
        let query = query_map(uri);
        let domain = query.get("domain").map(|v| v.to_string());
        let commit = query.get("commit").map(|v| v.to_string());
        let threshold = query.get("threshold").map(|v| v.parse::<f32>().unwrap());
        let candidates = query.get("candidates").map(|v| v.parse::<usize>().unwrap());
        match (domain, commit) {
            (Some(domain), Some(commit)) => Ok(ResourceSpec::DuplicateCandidates {
                domain,
                commit,
                threshold,
                candidates,
            }),
            _ => Err(SpecParseError::NoCommitIdOrDomain),
        }
    } else if RE_STATISTICS.is_match(path) {
        Ok(ResourceSpec::GetStatistics)
    } else {
        Err(SpecParseError::UnknownPath)
    }
}

#[derive(Clone, Debug)]
pub enum TaskStatus {
    Pending {
        progress: f32,
        start_time: DateTime<Utc>,
        num_retries: usize,
    },
    Error {
        message: String,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        num_retries: usize,
    },
    Completed {
        indexed_documents: usize,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        num_retries: usize,
    },
}

impl TaskStatus {
    pub fn start_time(&self) -> DateTime<Utc> {
        match self {
            TaskStatus::Pending { start_time, .. } => *start_time,
            TaskStatus::Error { start_time, .. } => *start_time,
            TaskStatus::Completed { start_time, .. } => *start_time,
        }
    }
    pub fn end_time(&self) -> Option<DateTime<Utc>> {
        match self {
            TaskStatus::Pending { .. } => None,
            TaskStatus::Error { end_time, .. } => Some(*end_time),
            TaskStatus::Completed { end_time, .. } => Some(*end_time),
        }
    }
    pub fn num_retries(&self) -> usize {
        match self {
            TaskStatus::Pending { num_retries, .. } => *num_retries,
            TaskStatus::Error { num_retries, .. } => *num_retries,
            TaskStatus::Completed { num_retries, .. } => *num_retries,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryResult {
    id: String,
    distance: f32,
}

pub struct Service {
    content_endpoint: Option<String>,
    user_forward_header: String,
    path: PathBuf,
    vector_store: Arc<VectorStore>,
    pending: Mutex<HashSet<String>>,
    tasks: RwLock<HashMap<String, TaskStatus>>,
    indexes: RwLock<HashMap<String, Arc<OpenAIHnsw>>>,
}

#[derive(Debug, Error)]
enum StartIndexError {
    #[error("No content endpoint found: specify at server startup or supply indexing data from the command line")]
    NoContentEndpoint,
}

async fn extract_body(req: Request<Body>) -> Bytes {
    hyper::body::to_bytes(req.into_body()).await.unwrap()
}

enum TerminusIndexOperationError {}

async fn get_operations_from_content_endpoint(
    content_endpoint: String,
    user_forward_header: String,
    domain: String,
    commit: String,
    previous: Option<String>,
) -> Result<impl Stream<Item = io::Result<Operation>> + Unpin, io::Error> {
    let mut params: Vec<_> = [("commit_id", commit)].into_iter().collect();
    if let Some(previous) = previous {
        params.push(("previous", previous))
    }
    let endpoint = format!("{}/{}", content_endpoint, &domain);
    let url = reqwest::Url::parse_with_params(&endpoint, &params).unwrap();
    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header(user_forward_header, "admin")
        .send()
        .await
        .unwrap();
    let status = res.status();
    if status != StatusCode::OK {
        let raw_s = res
            .bytes()
            .await
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
        let s = String::from_utf8_lossy(&raw_s);
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "terminusdb indexer endpoint failed with status code {}:\n{}",
                status, s
            ),
        ))
    } else {
        let res = res
            .bytes_stream()
            .map(|bytes| {
                eprintln!("fetched {:?} bytes", bytes.as_ref().map(|b| b.len()));
                bytes
            })
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e));
        let lines = StreamReader::new(res).lines();
        let lines_stream = LinesStream::new(lines);
        let fp = lines_stream.and_then(|l| {
            future::ready(
                serde_json::from_str(&l).map_err(|e| std::io::Error::new(ErrorKind::Other, e)),
            )
        });
        Ok(fp)
    }
}

#[derive(Debug, Error)]
enum ResponseError {
    #[error("{0:?}")]
    HeaderError(#[from] HeaderError),
    #[error("{0:?}")]
    IoError(#[from] std::io::Error),
    #[error("{0:?}")]
    SerdeError(#[from] serde_json::Error),
    #[error("{0:?}")]
    StartIndexError(#[from] StartIndexError),
    #[error("{0:?}")]
    SearchError(#[from] SearchError),
    #[error("Missing id in index {0}")]
    IdMissing(String),
    #[error("Embedding error: {0:?}")]
    EmbeddingError(#[from] EmbeddingError),
    #[error("source commit not found")]
    SourceCommitNotFound,
    #[error("target commit already has an index")]
    TargetCommitAlreadyHasIndex,
    #[error("{0:?}")]
    SerializationError(#[from] SerializationError),
    #[error("{0:?}")]
    JoinError(#[from] JoinError),
    #[error("{0:?}")]
    IndexError(#[from] IndexError),
}

fn add_to_duplicates(duplicates: &mut HashMap<usize, usize>, id1: usize, id2: usize) {
    if id1 < id2 {
        duplicates.insert(id1, id2);
    }
}

impl Service {
    async fn get_task_status(&self, task_id: &str) -> Option<TaskStatus> {
        self.tasks.read().await.get(task_id).cloned()
    }

    async fn set_task_status(&self, task_id: String, status: TaskStatus) {
        self.tasks.write().await.insert(task_id, status);
    }

    async fn get_index(&self, index_id: &str) -> Result<Arc<OpenAIHnsw>, ResponseError> {
        if let Some(hnsw) = self.indexes.read().await.get(index_id) {
            Ok(hnsw.clone())
        } else {
            let mut path = self.path.clone();
            let domain = self
                .vector_store
                .get_domain_sized(index_id, Model::Ada2.size())?;
            let index_path = index_serialization_path(path, index_id);
            Ok(Arc::new(OpenAIHnsw::deserialize(
                index_path,
                self.vector_store.clone(),
            )?))
        }
    }

    async fn set_index(&self, index_id: String, hnsw: Arc<OpenAIHnsw>) {
        self.indexes.write().await.insert(index_id, hnsw);
    }

    async fn test_and_set_pending(&self, index_id: String) -> bool {
        let mut lock = self.pending.lock().await;
        if lock.contains(&index_id) {
            false
        } else {
            lock.insert(index_id);
            true
        }
    }

    async fn clear_pending(&self, index_id: &str) {
        self.pending.lock().await.remove(index_id);
    }

    fn generate_task() -> String {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        s
    }

    fn new<P: Into<PathBuf>>(
        path: P,
        user_forward_header: String,
        num_bufs: usize,
        content_endpoint: Option<String>,
    ) -> Self {
        let path = path.into();
        Service {
            content_endpoint,
            user_forward_header,
            path: path.clone(),
            vector_store: Arc::new(VectorStore::new(path, num_bufs)),
            pending: Mutex::new(HashSet::new()),
            tasks: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
        }
    }

    async fn serve(self: Arc<Self>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        eprintln!(
            "{:?}: {:?} {:?}",
            chrono::offset::Local::now(),
            req.method(),
            req.uri()
        );
        match *req.method() {
            Method::POST => self.post(req).await,
            Method::GET => self.get(req).await,
            _ => todo!(),
        }
    }

    async fn load_hnsw_for_indexing(
        &self,
        idxid: IndexIdentifier,
    ) -> Result<Arc<OpenAIHnsw>, ResponseError> {
        if let Some(previous_id) = idxid.previous {
            //let commit = idxid.commit;
            let domain = idxid.domain;
            let previous_id = create_index_name(&domain, &previous_id);
            self.get_index(&previous_id).await
        } else {
            Err(ResponseError::SerializationError(
                SerializationError::IndexNotFound,
            ))
        }
    }

    async fn start_indexing_inner(
        self: Arc<Self>,
        domain: String,
        commit: String,
        previous: Option<String>,
        task_id: &str,
        api_key: String,
        model: Model,
        index_id: &str,
        content_endpoint: String,
    ) -> Result<(String, Arc<OpenAIHnsw>), ResponseError> {
        let internal_task_id = task_id;
        let opstream = get_operations_from_content_endpoint(
            content_endpoint.to_string(),
            self.user_forward_header.clone(),
            domain.clone(),
            commit.clone(),
            previous.clone(),
        )
        .await?
        .chunks(100);
        /*
        self.process_operation_chunks(
            opstream, domain, commit, previous, index_id, task_id, &api_key, model,
        )
        .await
        */
        todo!();
    }

    fn start_indexing(
        self: Arc<Self>,
        domain: String,
        commit: String,
        previous: Option<String>,
        task_id: String,
        api_key: String,
        model: Model,
    ) -> Result<(), StartIndexError> {
        let content_endpoint = self.content_endpoint.clone();
        let internal_task_id = task_id.clone();
        if let Some(content_endpoint) = content_endpoint {
            tokio::spawn(async move {
                let index_id = create_index_name(&domain, &commit);
                if self.test_and_set_pending(index_id.clone()).await {
                    self.set_task_status(
                        internal_task_id.clone(),
                        TaskStatus::Pending {
                            progress: 0.0,
                            start_time: Utc::now(),
                            num_retries: 0,
                        },
                    )
                    .await;
                    let result = self
                        .clone()
                        .start_indexing_inner(
                            domain,
                            commit,
                            previous,
                            &task_id,
                            api_key,
                            model,
                            &index_id,
                            content_endpoint,
                        )
                        .await;
                    let old_task = self.get_task_status(&internal_task_id).await.unwrap();

                    match result {
                        Ok((id, hnsw)) => {
                            let layer_len = hnsw.layer_count();
                            self.set_index(id, hnsw).await;
                            self.set_task_status(
                                task_id,
                                TaskStatus::Completed {
                                    indexed_documents: layer_len,
                                    start_time: old_task.start_time(),
                                    end_time: Utc::now(),
                                    num_retries: old_task.num_retries(),
                                },
                            )
                            .await;
                            self.clear_pending(&index_id).await;
                        }
                        Err(err) => {
                            eprintln!(
                                "{:?}: error while indexing: {:?}",
                                chrono::offset::Local::now(),
                                err
                            );
                            self.set_task_status(
                                internal_task_id,
                                TaskStatus::Error {
                                    message: err.to_string(),
                                    start_time: old_task.start_time(),
                                    end_time: Utc::now(),
                                    num_retries: old_task.num_retries(),
                                },
                            )
                            .await;
                            self.clear_pending(&index_id).await;
                        }
                    }
                }
            });
            Ok(())
        } else {
            Err(StartIndexError::NoContentEndpoint)
        }
    }

    async fn assign_index(
        self: Arc<Self>,
        domain: String,
        source_commit: String,
        target_commit: String,
    ) -> Result<(), ResponseError> {
        let source_name = create_index_name(&domain, &source_commit);
        let target_name = create_index_name(&domain, &target_commit);
        let index = self.get_index(&source_name).await?;
        let mut indexes = self.indexes.write().await;
        indexes.insert(target_name.clone(), index.clone());
        std::mem::drop(indexes);
        tokio::task::block_in_place(move || {
            let file_name = index_serialization_path(&self.path, &target_name);
            index.serialize(file_name).unwrap();
        });
        Ok(())
    }

    async fn get_start_index(
        self: Arc<Self>,
        req: Request<Body>,
        domain: String,
        commit: String,
        previous: Option<String>,
    ) -> Result<String, ResponseError> {
        let task_id = Service::generate_task();
        let api_key = get_header_value(req.headers(), "VECTORLINK_EMBEDDING_API_KEY")?;
        self.set_task_status(
            task_id.clone(),
            TaskStatus::Pending {
                progress: 0.0,
                start_time: Utc::now(),
                num_retries: 0,
            },
        );
        self.start_indexing(
            domain,
            commit,
            previous,
            task_id.clone(),
            api_key,
            Model::Ada2,
        )?;
        Ok(task_id)
    }

    async fn get(self: Arc<Self>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let uri = req.uri();
        match uri_to_spec(uri) {
            Ok(ResourceSpec::StartIndex {
                domain,
                commit,
                previous,
            }) => {
                let result = self.get_start_index(req, domain, commit, previous).await;
                string_response_or_error(result)
            }
            Ok(ResourceSpec::AssignIndex {
                domain,
                source_commit,
                target_commit,
            }) => {
                let result = self
                    .assign_index(domain, source_commit, target_commit)
                    .await;
                match result {
                    Ok(()) => Ok(Response::builder().status(204).body(Body::empty()).unwrap()),
                    Err(e) => Ok(Response::builder()
                        .status(400)
                        .body(e.to_string().into())
                        .unwrap()),
                }
            }
            Ok(ResourceSpec::CheckTask { task_id }) => {
                if let Some(state) = self.get_task_status(&task_id).await {
                    match state {
                        TaskStatus::Pending {
                            progress,
                            start_time,
                            num_retries,
                        } => {
                            let obj = json!({"status":"Pending","percentage":progress, "start":start_time.to_rfc3339(), "retries": num_retries});
                            Ok(Response::builder().body(obj.to_string().into()).unwrap())
                        }
                        TaskStatus::Error {
                            message,
                            start_time,
                            end_time,
                            num_retries,
                        } => {
                            // blah
                            let elapsed = end_time - start_time;
                            let obj = json!({"status":"Error", "start":start_time.to_rfc3339(), "end":end_time.to_rfc3339(), "elapsed": elapsed.to_string(), "retries": num_retries, "message": message});

                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(obj.to_string().into())
                                .unwrap())
                        }

                        TaskStatus::Completed {
                            indexed_documents,
                            start_time,
                            end_time,
                            num_retries,
                        } => {
                            let elapsed = end_time - start_time;
                            let obj = json!({"status":"Complete", "start":start_time.to_rfc3339(), "end":end_time.to_rfc3339(), "elapsed": elapsed.to_string(),"indexed_documents":indexed_documents, "retries":num_retries});
                            Ok(Response::builder().body(obj.to_string().into()).unwrap())
                        }
                    }
                } else {
                    Ok(Response::builder().status(404).body(Body::empty()).unwrap())
                }
            }
            Ok(ResourceSpec::DuplicateCandidates {
                domain,
                commit,
                threshold,
                candidates,
            }) => {
                let candidates = candidates.unwrap_or(2);
                let result = self
                    .get_duplicate_candidates(domain, commit, threshold, candidates)
                    .await;
                string_response_or_error(result)
            }
            Ok(ResourceSpec::Similar {
                domain,
                commit,
                count,
                id,
            }) => {
                let result = self.get_similar_documents(domain, commit, id, count).await;
                string_response_or_error(result)
            }
            Ok(ResourceSpec::GetStatistics) => {
                todo!();
            }
            Ok(_) => todo!(),
            Err(e) => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(e.to_string().into())
                .unwrap()),
        }
    }

    async fn get_similar_documents(
        self: Arc<Self>,
        domain: String,
        commit: String,
        id: String,
        count: usize,
    ) -> Result<String, ResponseError> {
        let index_id = create_index_name(&domain, &commit);
        // if None, then return 404
        let hnsw = self.get_index(&index_id).await?;
        let elts = hnsw.layer_count();
        todo!();
        /*
            let qp = (0..elts)
                .into_par_iter()
                .find_first(|i| hnsw.feature(*i).id() == id)
                .map(|i| hnsw.feature(i));

            match qp {
                Some(qp) => {
                    let res = search(qp, count, &hnsw);
                    let ids: Vec<QueryResult> = res
                        .par_iter()
                        .map(|p| QueryResult {
                            id: p.id().to_string(),
                            distance: p.distance(),
                        })
                        .collect();
                    let s = serde_json::to_string(&ids)?;
                    Ok(s)
                }
                None => Err(ResponseError::IdMissing(id)),
        }
            */
    }

    async fn get_duplicate_candidates(
        self: Arc<Self>,
        domain: String,
        commit: String,
        threshold: Option<f32>,
        candidates: usize,
    ) -> Result<String, ResponseError> {
        let index_id = create_index_name(&domain, &commit);
        // if None, then return 404
        let hnsw = self.get_index(&index_id).await?;
        todo!();

        /*
        let elts: AllVectorIterator<'_> = hnsw.all_vectors();
        let clusters: Vec<(usize, Vec<(usize, f32)>)> = elts
            .par_bridge()
            .map(|i| {
                let current_point = AbstractVector::Stored(i);
                let results = hnsw.search(current_point, candidates + 1);
                let mut cluster = Vec::new();
                for result in results.iter() {
                    if result.0 != i {
                        let distance = result.1;
                        if distance < threshold.unwrap_or(f32::MAX) {
                            cluster.push((result.0, distance))
                        }
                    }
                }
                (i, cluster)
            })
            .collect();
        let mut v: Vec<(&str, Vec<(&str, f32)>)> = clusters
            .into_iter()
            .map(|(i, vjs)| {
                let vns = vjs
                    .iter()
                    .map(|(j, f)| (hnsw.feature(*j).id(), *f))
                    .collect();
                (hnsw.feature(i).id(), vns)
            })
        .collect();
        let result = serde_json::to_string(&v)?;
        Ok(result)
        */
    }

    async fn post(&self, req: Request<Body>) -> Result<Response<Body>, Infallible> {
        let uri = req.uri();
        match uri_to_spec(uri) {
            Ok(ResourceSpec::Search {
                domain,
                commit,
                count,
            }) => {
                let headers = req.headers().clone();
                let body = req.into_body();
                let body_bytes = hyper::body::to_bytes(body).await.unwrap();
                let q = String::from_utf8(body_bytes.to_vec()).unwrap();
                let api_key = get_header_value(&headers, "VECTORLINK_EMBEDDING_API_KEY");
                let result: Result<Response<Body>, ResponseError> = self
                    .index_response(api_key, q, domain, commit, count, Model::Ada2)
                    .await;
                match result {
                    Ok(body) => Ok(body),
                    Err(e) => Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(e.to_string().into())
                        .unwrap()),
                }
            }
            Ok(_) => todo!(),
            Err(e) => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(e.to_string().into())
                .unwrap()),
        }
    }

    async fn index_response(
        &self,
        api_key: Result<String, HeaderError>,
        q: String,
        domain: String,
        commit: String,
        count: usize,
        model: Model,
    ) -> Result<Response<Body>, ResponseError> {
        let api_key = api_key?;
        let vec: Vec<[f32; 1536]> = embeddings_for(&api_key, &[q], model).await?.0;
        let qp = Point::Mem {
            vec: Box::new(vec[0]),
        };
        let index_id = create_index_name(&domain, &commit);
        // if None, then return 404
        let hnsw = self.get_index(&index_id).await?;
        let res = search(&qp, count, &hnsw);
        let ids: Vec<QueryResult> = res
            .iter()
            .map(|p| QueryResult {
                id: p.internal_id().to_string(),
                distance: p.distance(),
            })
            .collect();
        let s = serde_json::to_string(&ids)?;
        Ok(Response::builder().body(s.into()).unwrap())
    }
}

fn string_response_or_error(
    result: Result<String, ResponseError>,
) -> Result<Response<Body>, Infallible> {
    match result {
        Ok(task_id) => Ok(Response::builder().body(task_id.into()).unwrap()),
        Err(e) => Ok(Response::builder()
            .status(400)
            .body(e.to_string().into())
            .unwrap()),
    }
}

fn json_response_or_error(
    result: Result<String, ResponseError>,
) -> Result<Response<Body>, Infallible> {
    match result {
        Ok(task_id) => Ok(Response::builder()
            .header("Content-Type", "application/json")
            .body(task_id.into())
            .unwrap()),
        Err(e) => Ok(Response::builder()
            .status(400)
            .body(e.to_string().into())
            .unwrap()),
    }
}

#[derive(Debug, Error)]
enum AssignIndexError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("source commit not found")]
    SourceCommitNotFound,
    #[error("target commit already has an index")]
    TargetCommitAlreadyHasIndex,
}

pub async fn serve<P: Into<PathBuf>>(
    directory: P,
    user_forward_header: String,
    port: u16,
    num_bufs: usize,
    content_endpoint: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port);
    let service = Arc::new(Service::new(
        directory,
        user_forward_header,
        num_bufs,
        content_endpoint,
    ));
    let make_svc = make_service_fn(move |_conn| {
        let s = service.clone();
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let s = s.clone();
                async move { s.serve(req).await }
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);
    server.await?;

    Ok(())
}
