use std::{
    collections::BTreeMap,
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use etcd_client::{Client, PutOptions, Txn, TxnOp};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::StreamExt;

use crate::{
    key::{concat_bytes, task_key},
    queue::Queue,
};

use prometheus::core::{AtomicF64, GenericCounter, GenericCounterVec};
type C = GenericCounter<AtomicF64>;
type CV = GenericCounterVec<AtomicF64>;
use prometheus_exporter::{
    self,
    prometheus::{register_counter, register_counter_vec, TextEncoder, gather},
};

const PUSHGATEWAY_IP: &str = "http://localhost:9091";

#[derive(Clone)]
pub struct Task {
    client: Client,
    task_id: String,
    task_key: Vec<u8>,
    claim_key: Vec<u8>,
    interrupt_key: Vec<u8>,
    queue_identity: String,
    lease: Option<i64>,
    state: TaskData,
    last_renew: SystemTime,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<task {} ({:?})>",
            String::from_utf8_lossy(&self.task_key),
            self.state.status
        )
    }
}
const RENEW_DURATION: Duration = Duration::from_secs(1);

async fn get_task_state(client: &mut Client, task_key: &[u8]) -> Result<TaskData, TaskStateError> {
    let response = client.get(task_key, None).await?;
    let data = response.kvs()[0].value();
    let deserialized: TaskData = serde_json::from_reader(data)?;

    Ok(deserialized)
}

#[derive(Debug, Error)]
#[error("Lease expired")]
pub struct LeaseExpired;

impl From<etcd_client::Error> for LeaseExpired {
    fn from(_value: etcd_client::Error) -> Self {
        Self
    }
}

impl From<LeaseExpired> for TaskStateError {
    fn from(_value: LeaseExpired) -> Self {
        TaskStateError::LeaseExpired
    }
}

async fn send_keep_alive(client: &mut Client, lease: i64) -> Result<(), LeaseExpired> {
    eprintln!("sending a keepalive");
    let (mut lease, mut lease_stream) = client.lease_keep_alive(lease).await?;
    lease.keep_alive().await?;
    let result = lease_stream
        .try_next()
        .await?
        .expect("no keep alive confirmation received");

    if result.ttl() == 0 {
        eprintln!("!!!!LEASE EXPIRED!!!!!");
        Err(LeaseExpired)
    } else {
        Ok(())
    }
}

async fn keep_alive_continuously(
    mut client: Client,
    lease: i64,
    canary: Arc<AtomicBool>,
) -> Result<(), LeaseExpired> {
    let mut interval_stream = tokio::time::interval(Duration::from_secs(1));
    interval_stream.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while canary.load(atomic::Ordering::Relaxed) {
        interval_stream.tick().await;
        eprintln!("keeping alive..");
        send_keep_alive(&mut client, lease).await?;
    }

    Ok(())
}

impl Task {
    pub async fn new(
        queue: &Queue,
        task_id: String,
        lease: Option<i64>,
    ) -> Result<Self, TaskStateError> {
        let task_key = concat_bytes(&queue.tasks_prefix, task_id.as_bytes());
        let claim_key = concat_bytes(&queue.claims_prefix, task_id.as_bytes());
        let interrupt_key = concat_bytes(&queue.interrupt_prefix, task_id.as_bytes());
        let queue_identity = queue.identity.clone();
        let mut client = queue.client.clone();
        let state = get_task_state(&mut client, &task_key[..]).await?;
        Ok(Self {
            client: queue.client.clone(),
            task_id,
            task_key,
            claim_key,
            interrupt_key,
            queue_identity,
            lease,
            state,
            last_renew: SystemTime::now(),
        })
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub async fn alive(&mut self) -> Result<(), TaskStateError> {
        if self.lease.is_none() {
            panic!("tried to lease a task that was initialized without lease");
        }

        if RENEW_DURATION < self.last_renew.elapsed().unwrap() {
            send_keep_alive(&mut self.client, self.lease.unwrap()).await?;

            let interrupt = self.client.get(&self.interrupt_key[..], None).await?;
            if let Some(first) = interrupt.kvs().first() {
                let next_status = match first.key() {
                    b"canceled" => TaskStatus::Canceled,
                    b"paused" => TaskStatus::Paused,
                    _ => panic!("unknown interrupt reason"),
                };

                let delete_interrupt = vec![TxnOp::delete(&self.interrupt_key[..], None)];
                self.state.status = next_status;
                self.update_state_noalive(delete_interrupt).await?;
            }

            self.last_renew = SystemTime::now();
        }

        Ok(())
    }

    async fn release_claim(&mut self) -> Result<(), etcd_client::Error> {
        if let Some(lease) = self.lease {
            let _response = self.client.lease_revoke(lease).await?;
        }
        Ok(())
    }

    pub async fn refresh_state(&mut self) -> Result<(), TaskStateError> {
        if self.lease.is_some() {
            // Only refresh lease if we are the owner of this task.
            // It is allowed to look at tasks without claiming them.
            self.alive().await?;
        }
        let response = self.client.get(&self.task_key[..], None).await?;
        let data = response.kvs()[0].value_str().unwrap().to_owned();
        let deserialized: TaskData = serde_json::from_str(&data)?;
        self.state = deserialized;

        Ok(())
    }

    async fn update_state_noalive(
        &mut self,
        extra_success_ops: Vec<TxnOp>,
    ) -> Result<(), TaskStateError> {
        let data = serde_json::to_string_pretty(&self.state)?;
        let mut success_ops = vec![
            TxnOp::put(
                &self.claim_key[..],
                self.queue_identity.as_bytes(),
                Some(PutOptions::new().with_lease(self.lease.unwrap())),
            ),
            TxnOp::put(&self.task_key[..], data, None),
        ];

        success_ops.extend(extra_success_ops);
        self.client.txn(Txn::new().and_then(success_ops)).await?;

        Ok(())
    }

    async fn update_state(&mut self, extra_success_ops: Vec<TxnOp>) -> Result<(), TaskStateError> {
        self.alive().await?;
        self.update_state_noalive(extra_success_ops).await
    }

    pub fn state(&self) -> &TaskData {
        &self.state
    }

    pub fn status(&self) -> TaskStatus {
        self.state.status
    }

    fn typed_field<T: DeserializeOwned>(
        &self,
        field: &str,
    ) -> Result<Option<T>, serde_json::Error> {
        match self
            .state
            .other_fields
            .get(field)
            .map(|v| serde_json::from_value(v.clone()))
        {
            None => Ok(None),
            Some(Ok(p)) => Ok(Some(p)),
            Some(Err(e)) => Err(e),
        }
    }

    fn set_typed_field<T: Serialize>(
        &mut self,
        field: String,
        value: T,
    ) -> Result<(), TaskStateError> {
        self.state
            .other_fields
            .insert(field, serde_json::to_value(value)?);

        Ok(())
    }

    pub fn init<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.typed_field("init")
    }

    pub fn progress<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.typed_field("progress")
    }

    pub fn result<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.typed_field("result")
    }

    pub fn error<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.typed_field("error")
    }

    fn verify_status(&self, expected: TaskStatus) {
        if self.status() != expected {
            panic!(
                "expected status {expected:?} but task {} was in status {:?}",
                self.task_id,
                self.status()
            );
        }
    }

    async fn transition_to_status(
        &mut self,
        from: TaskStatus,
        to: TaskStatus,
    ) -> Result<(), TaskStateError> {
        self.verify_status(from);
        self.set_status(to).await?;
        Ok(())
    }

    async fn set_status(&mut self, to: TaskStatus) -> Result<(), TaskStateError> {
        self.state.status = to;
        self.update_state(Vec::new()).await
    }

    pub async fn set_progress<T: Serialize>(&mut self, progress: T) -> Result<(), TaskStateError> {
        self.set_typed_field("progress".to_owned(), progress)?;
        self.update_state(Vec::new()).await
    }

    pub async fn start(&mut self) -> Result<(), TaskStateError> {
        self.transition_to_status(TaskStatus::Pending, TaskStatus::Running)
            .await
    }

    pub async fn resume(&mut self) -> Result<(), TaskStateError> {
        self.transition_to_status(TaskStatus::Resuming, TaskStatus::Running)
            .await
    }

    pub async fn finish<T: Serialize>(&mut self, result: T) -> Result<(), TaskStateError> {
        self.set_typed_field("result".to_owned(), result)?;
        self.transition_to_status(TaskStatus::Running, TaskStatus::Complete)
            .await?;
        self.release_claim().await?;

        Ok(())
    }

    pub async fn finish_error<T: Serialize>(&mut self, error: T) -> Result<(), TaskStateError> {
        self.set_typed_field("error".to_owned(), error)?;
        self.transition_to_status(TaskStatus::Running, TaskStatus::Error)
            .await?;
        self.release_claim().await?;

        Ok(())
    }

    pub async fn spawn_child<T: Serialize>(
        &mut self,
        queue: &str,
        task_id: &str,
        init: &T,
    ) -> Result<(), TaskStateError> {
        let full_self_id = format!("{}/{}", self.queue_identity, self.task_id);
        let task_key = task_key(format!("{queue}/{task_id}").as_bytes());

        let mut version = 0;
        // best to start with checking if a child is spawnable at all
        let result = self.client.get(task_key, None).await?;
        if !result.kvs().is_empty() {
            // the key is there but we might still be able to do this!
            // allow task creation if the task is pending or final.
            // deny if task is currently running, resuming, waiting or paused.
            //
            // If the task is unparsable, that is considered
            // equivalent to an error state, and therefore overwriting
            // it is fine.
            version = result.kvs()[0].version();

            if let Ok(task_data) = serde_json::from_reader::<_, TaskData>(result.kvs()[0].value()) {
                if !task_data.status.is_final() {
                    return Err(TaskStateError::TaskAlreadyRunning);
                }
            }
        }

        // since we got here, it should be fine to overwrite. as long as the version is the same.

        let task_data = TaskData {
            status: TaskStatus::Pending,
            parent: Some(full_self_id),
            children: None,
            other_fields: BTreeMap::new(),
        };

        // make extra success ops be about creating the tasks
        // self.update_state(extra_success_ops);
        todo!();
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Resuming,
    Running,
    Waiting,
    Paused,
    Complete,
    Error,
    Canceled,
}

impl TaskStatus {
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            TaskStatus::Complete | TaskStatus::Error | TaskStatus::Canceled
        )
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::Resuming => write!(f, "resuming"),
            TaskStatus::Running => write!(f, "running"),
            TaskStatus::Waiting => write!(f, "waiting"),
            TaskStatus::Paused => write!(f, "paused"),
            TaskStatus::Complete => write!(f, "complete"),
            TaskStatus::Error => write!(f, "error"),
            TaskStatus::Canceled => write!(f, "canceled"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskData {
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<String>>,
    #[serde(flatten)]
    pub other_fields: BTreeMap<String, serde_json::Value>,
}

pub enum InterruptReason {
    Paused,
    Canceled,
}

#[derive(Debug, Error)]
pub enum TaskAliveError {
    #[error("task was interrupted")]
    Interrupted,
    #[error("keepalive failed: {0:?}")]
    KeepAliveFailed(#[from] etcd_client::Error),
}

#[derive(Debug, Error)]
pub enum TaskStateError {
    #[error("lease expired")]
    LeaseExpired,
    #[error(transparent)]
    Etcd(#[from] etcd_client::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Alive(#[from] TaskAliveError),
    #[error("tried to create a task that is already running")]
    TaskAlreadyRunning,
}

#[async_trait]
pub trait TaskHandler
where
    Self: 'static,
{
    type Init: DeserializeOwned + Send + 'static;
    type Progress: Serialize + DeserializeOwned + Send + 'static;
    type Complete: Serialize + Send + 'static;

    type Error: Serialize + Send + 'static;

    async fn initialize(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error>;
    async fn process(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error>;

    fn start_prometheus_exporter() -> () {
        let binding = "127.0.0.1:9002".parse().unwrap();
        prometheus_exporter::start(binding).unwrap();
    }

    struct Metrics {
        tasks_claimed: CV,
        tasks_started: C,
        tasks_spawned: C,
        tasks_resumed: C,
        errors_spawned: C,
        tasks_finished_ok: C,
        tasks_finished_err: C,
    }

    fn register_metrics() -> (CV, C, C, C, C, C, C) {
        let tasks_claimed = register_counter_vec!("task_claimed_counter", "Number of tasks claimed", &["status", "task_id"]).unwrap();
        let tasks_started = register_counter!("task_started_counter", "Number of tasks started").unwrap();
        let tasks_spawned = register_counter!("task_spawned_counter", "Number of tasks spawned").unwrap();
        let tasks_resumed = register_counter!("task_resumed_counter", "Number of tasks resumed").unwrap();
        let errors_spawned = register_counter!("spawn_error_counter", "Number of tasks that encountered an error during spawn").unwrap();
        let tasks_finished_ok = register_counter!("task_finish_ok_counter", "Number of tasks that finished successfully").unwrap();
        let tasks_finished_err = register_counter!("task_finish_err_counter", "Number of tasks that finished with an error").unwrap();
        Metrics {
            tasks_claimed,
            tasks_started,
            tasks_spawned,
            tasks_resumed,
            errors_spawned,
            tasks_finished_ok,
            tasks_finished_err,
        }
    }

    async fn process_queue(queue: &mut Queue) -> Result<(), TaskStateError> {
        Self::start_prometheus_exporter();
        let mut metrics = Self::register_metrics();

        let metric_families = gather();
        let encoder = TextEncoder::new();
        encoder.encode_to_string(&metric_families).unwrap(); // not sure yet if this is necessary

        loop {
            let mut task = queue.next_task().await?;
            metrics.tasks_claimed.with_label_values(&[&task.status().to_string(), &task.task_id()]).inc();
            // todo, the clone here is not really desirable. we need a way to get the liveness without copying a full task
            match task.status() {
                TaskStatus::Pending => {
                    task.start().await?;
                    metrics.tasks_started.inc();
                    let init_live = TaskLiveness::new(task.clone());
                    match tokio::task::spawn(Self::initialize(init_live)).await {
                        Ok(Ok(progress)) => {
                            task.set_progress(progress).await?;
                        }
                        Ok(Err(e)) => {
                            task.finish_error(e).await?;
                            metrics.errors_spawned.inc();
                            // end task
                            continue;
                        }
                        Err(e) => {
                            match e.try_into_panic() {
                                Ok(panic) => {
                                    task.finish_error(format!("task panicked: {panic:?}"))
                                        .await?
                                }
                                Err(e) => task.finish_error(e.to_string()).await?,
                            };
                            metrics.errors_spawned.inc();
                            // end task
                            continue;
                        }
                    }
                    metrics.tasks_spawned.inc();
                }
                TaskStatus::Resuming => {
                    task.resume().await?;
                    metrics.tasks_resumed.inc();
                }
                _ => panic!("task was not in proper state"),
            };

            let live = TaskLiveness::new(task.clone());
            let spawned_handler = tokio::task::spawn(Self::process(live));

            let result = spawned_handler.await;
            task.refresh_state().await?;
            
            match result {
                Ok(Ok(c)) => {
                    task.finish(c).await?;
                    metrics.tasks_finished_ok.inc();
                }
                Ok(Err(e)) => {
                    task.finish_error(e).await?;
                    metrics.tasks_finished_err.inc();
                }
                Err(e) => {
                    task.finish_error(e.to_string()).await?;
                    metrics.tasks_finished_err.inc();
                }
            }
        }
    }
}

pub struct TaskLiveness<Init, Progress> {
    task: Task,
    _init: PhantomData<Init>,
    _progress: PhantomData<Progress>,
}

impl<Init: DeserializeOwned, Progress: Serialize + DeserializeOwned + Send + 'static>
    TaskLiveness<Init, Progress>
{
    fn new(task: Task) -> Self {
        Self {
            task,
            _init: PhantomData,
            _progress: PhantomData,
        }
    }

    pub async fn keepalive(&mut self) -> Result<(), TaskStateError> {
        self.task.alive().await
    }
    pub fn init(&self) -> Result<Option<Init>, serde_json::Error> {
        self.task.init()
    }

    pub fn progress(&self) -> Result<Option<Progress>, serde_json::Error> {
        self.task.progress()
    }

    pub async fn set_progress(&mut self, progress: Progress) -> Result<(), TaskStateError> {
        self.task.set_progress(progress).await?;

        Ok(())
    }

    pub fn into_sync(mut self) -> Result<SyncTaskLiveness<Init, Progress>, serde_json::Error> {
        let init = self.init()?;
        let progress = self.progress()?;
        let (send, mut receive) = mpsc::channel::<(
            ExchangeItemInner<Progress>,
            oneshot::Sender<Result<(), TaskStateError>>,
        )>(1);
        let task = tokio::spawn(async move {
            while let Some((progress, return_channel)) = receive.recv().await {
                let result = match progress {
                    ExchangeItemInner::Progress(progress) => self.task.set_progress(progress).await,
                    ExchangeItemInner::SendKeepalive => self.task.alive().await,
                    ExchangeItemInner::KeepAliveContinuously(canary) => {
                        println!("time to start a continous keepalive!");
                        let result = self.task.alive().await;
                        if result.is_ok() {
                            tokio::spawn(keep_alive_continuously(
                                self.task.client.clone(),
                                self.task.lease.unwrap(),
                                canary,
                            ));
                        }

                        result
                    }
                };
                return_channel.send(result).unwrap();
            }
        });
        Ok(SyncTaskLiveness {
            channel: send,
            task_handle: task,
            init,
            progress,
        })
    }

    pub async fn guarded_keepalive(&self) -> Result<LivenessGuard, TaskStateError> {
        let canary = Arc::new(AtomicBool::new(true));
        let canary2 = canary.clone();

        let mut client = self.task.client.clone();
        let lease = self.task.lease.unwrap();

        send_keep_alive(&mut client, lease).await?;

        let handle = tokio::spawn(keep_alive_continuously(client, lease, canary));

        Ok(LivenessGuard {
            canary: canary2,
            handle: Some(handle),
            expecting_liveness: true,
        })
    }
}

enum ExchangeItemInner<Progress> {
    SendKeepalive,
    KeepAliveContinuously(Arc<AtomicBool>),
    Progress(Progress),
}

type ExchangeItem<Progress> = (
    ExchangeItemInner<Progress>,
    oneshot::Sender<Result<(), TaskStateError>>,
);

pub struct SyncTaskLiveness<Init, Progress> {
    channel: mpsc::Sender<ExchangeItem<Progress>>,
    task_handle: JoinHandle<()>,
    init: Option<Init>,
    progress: Option<Progress>,
}

impl<Init, Progress> Drop for SyncTaskLiveness<Init, Progress> {
    fn drop(&mut self) {
        self.task_handle.abort();
    }
}

impl<Init, Progress: Clone + Send + 'static> SyncTaskLiveness<Init, Progress> {
    pub fn init(&self) -> Option<&Init> {
        self.init.as_ref()
    }

    pub fn progress(&self) -> Option<&Progress> {
        self.progress.as_ref()
    }

    fn send_progress(
        &mut self,
        progress: ExchangeItemInner<Progress>,
    ) -> Result<(), TaskStateError> {
        let (return_channel_sender, return_channel) = oneshot::channel();
        self.channel
            .blocking_send((progress, return_channel_sender))
            .unwrap();
        return_channel.blocking_recv().unwrap()
    }

    pub fn set_progress(&mut self, progress: Progress) -> Result<(), TaskStateError> {
        self.send_progress(ExchangeItemInner::Progress(progress.clone()))?;
        self.progress = Some(progress);

        Ok(())
    }

    pub fn keepalive(&mut self) -> Result<(), TaskStateError> {
        self.send_progress(ExchangeItemInner::SendKeepalive)
    }

    pub fn blocking_keepalive<T>(
        &mut self,
        mut func: impl FnMut() -> T,
    ) -> Result<T, TaskStateError> {
        let _keepalive = self.guarded_keepalive();
        let result = func();

        // do one final keepalive so we know we still have the lease as we leave the function
        self.keepalive()?;

        Ok(result)
    }

    pub fn guarded_keepalive(&mut self) -> Result<LivenessGuard, TaskStateError> {
        self.keepalive()?;
        let canary = Arc::new(AtomicBool::new(true));
        let canary2 = canary.clone();
        // result is safe to ignore here, as this always succeeds in the worker loop.
        let _ = self.send_progress(ExchangeItemInner::KeepAliveContinuously(canary));

        Ok(LivenessGuard {
            canary: canary2,
            handle: None,
            expecting_liveness: true,
        })
    }
}

pub struct LivenessGuard {
    canary: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<(), LeaseExpired>>>,
    expecting_liveness: bool,
}

impl LivenessGuard {
    pub fn join(mut self) -> Result<(), TaskStateError> {
        if self.expecting_liveness {
            self.expecting_liveness = false;
            if !self.canary.load(atomic::Ordering::Relaxed) {
                return Err(TaskStateError::LeaseExpired);
            }
        }

        Ok(())
    }
}

impl Drop for LivenessGuard {
    fn drop(&mut self) {
        if self.expecting_liveness && !self.canary.load(atomic::Ordering::Relaxed) {
            panic!("lease expired");
        }
        self.canary.store(false, atomic::Ordering::Relaxed);
    }
}

#[macro_export]
macro_rules! keepalive {
    ($live: expr, $body: expr) => {{
        {
            let guard = $live.guarded_keepalive().await.expect("keepalive failed");
            let result = $body;
            guard.join().expect("keepalive failed");

            result
        }
    }};
}

#[macro_export]
macro_rules! keepalive_sync {
    ($live: expr, $body: expr) => {{
        {
            let guard = $live.guarded_keepalive();
            let result = $body;
            guard.join().expect("keepalive failed");

            result
        }
    }};
}
