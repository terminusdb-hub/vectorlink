use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
    time::Duration,
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

use crate::queue::Queue;

#[derive(Clone)]
pub struct Task {
    client: Client,
    task_id: String,
    task_key: String,
    claim_key: String,
    interrupt_key: String,
    queue_identity: String,
    lease: Option<i64>,
    state: TaskData,
}

async fn get_task_state(client: &mut Client, task_key: &str) -> Result<TaskData, TaskStateError> {
    let response = client.get(task_key.as_bytes(), None).await?;
    let data = response.kvs()[0].value_str().unwrap().to_owned();
    let deserialized: TaskData = serde_json::from_str(&data)?;

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
    while canary.load(atomic::Ordering::Relaxed) {
        eprintln!("keeping alive..");
        send_keep_alive(&mut client, lease).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    Ok(())
}

impl Task {
    pub async fn new(
        queue: &Queue,
        task_id: String,
        lease: Option<i64>,
    ) -> Result<Self, TaskStateError> {
        let task_key = format!("{}{}", queue.tasks_prefix, task_id);
        let claim_key = format!("{}{}", queue.claims_prefix, task_id);
        let interrupt_key = format!("{}{}", queue.interrupt_prefix, task_id);
        let queue_identity = queue.identity.clone();
        let mut client = queue.client.clone();
        let state = get_task_state(&mut client, &task_key).await?;
        Ok(Self {
            client: queue.client.clone(),
            task_id,
            task_key,
            claim_key,
            interrupt_key,
            queue_identity,
            lease,
            state,
        })
    }

    pub async fn alive(&mut self) -> Result<(), TaskStateError> {
        if self.lease.is_none() {
            panic!("tried to lease a task that was initialized without lease");
        }
        send_keep_alive(&mut self.client, self.lease.unwrap()).await?;

        let interrupt = self.client.get(self.interrupt_key.as_bytes(), None).await?;
        if let Some(first) = interrupt.kvs().first() {
            let next_status = match first.key() {
                b"canceled" => TaskStatus::Canceled,
                b"paused" => TaskStatus::Paused,
                _ => panic!("unknown interrupt reason"),
            };

            let delete_interrupt = vec![TxnOp::delete(self.interrupt_key.as_bytes(), None)];
            self.state.status = next_status;
            self.update_state_noalive(delete_interrupt).await?;
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
        let response = self.client.get(self.task_key.as_bytes(), None).await?;
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
                self.claim_key.as_bytes(),
                self.queue_identity.as_bytes(),
                Some(PutOptions::new().with_lease(self.lease.unwrap())),
            ),
            TxnOp::put(self.task_key.as_bytes(), data, None),
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
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Resuming,
    Running,
    Paused,
    Complete,
    Error,
    Canceled,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskData {
    status: TaskStatus,
    #[serde(flatten)]
    other_fields: BTreeMap<String, serde_json::Value>,
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

    async fn process_queue(queue: &mut Queue) -> Result<(), TaskStateError> {
        loop {
            let mut task = queue.next_task().await?;
            // todo, the clone here is not really desirable. we need a way to get the liveness without copying a full task
            match task.status() {
                TaskStatus::Pending => {
                    task.start().await?;
                    let init_live = TaskLiveness::new(task.clone());
                    match tokio::task::spawn(Self::initialize(init_live)).await {
                        Ok(Ok(progress)) => {
                            task.set_progress(progress).await?;
                        }
                        Ok(Err(e)) => {
                            task.finish_error(e).await?;
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
                            // end task
                            continue;
                        }
                    }
                }
                TaskStatus::Resuming => {
                    task.resume().await?;
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
                }
                Ok(Err(e)) => {
                    task.finish_error(e).await?;
                }
                Err(e) => {
                    task.finish_error(e.to_string()).await?;
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

        tokio::spawn(keep_alive_continuously(client, lease, canary));

        Ok(LivenessGuard {
            canary: canary2,
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
            expecting_liveness: true,
        })
    }
}

pub struct LivenessGuard {
    canary: Arc<AtomicBool>,
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
            let guard = $live.guarded_keepalive().await;
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