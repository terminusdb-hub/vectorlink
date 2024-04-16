use std::collections::BTreeMap;

use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, PutOptions, Txn, TxnOp,
    WatchOptions,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio_stream::StreamExt;

pub struct Queue {
    #[allow(unused)]
    service_name: String,
    identity: String,
    client: Client,
    queue_prefix: String,
    tasks_prefix: String,
    claims_prefix: String,
    interrupt_prefix: String,
}

impl Queue {
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: Option<ConnectOptions>,
        service_name: String,
        identity: String,
    ) -> Result<Self, etcd_client::Error> {
        let client = Client::connect(endpoints, options).await?;

        let queue_prefix = format!("/services/queue/{service_name}");
        let tasks_prefix = format!("/services/tasks/{service_name}");
        let claims_prefix = format!("/services/claims/{service_name}");
        let interrupt_prefix = format!("/services/interrupt/{service_name}");
        Ok(Self {
            service_name,
            client,
            identity,
            queue_prefix,
            tasks_prefix,
            claims_prefix,
            interrupt_prefix,
        })
    }

    async fn claim_task(&mut self, task_id: String) -> Result<Option<Task>, TaskStateError> {
        let queue_key = format!("{}{}", self.queue_prefix, task_id);
        let claim_key = format!("{}{}", self.claims_prefix, task_id);

        let lease = self.client.lease_grant(10, None).await?;
        let result = self
            .client
            .txn(
                Txn::new()
                    .when([Compare::version(claim_key.as_bytes(), CompareOp::Equal, 0)])
                    .and_then([
                        TxnOp::delete(queue_key.as_bytes(), None),
                        TxnOp::put(
                            claim_key.as_bytes(),
                            self.identity.as_bytes(),
                            Some(PutOptions::new().with_lease(lease.id())),
                        ),
                    ])
                    .or_else([TxnOp::delete(queue_key.as_bytes(), None)]),
            )
            .await?;

        if result.succeeded() {
            Ok(Some(Task::new(self, task_id, Some(lease.id())).await?))
        } else {
            Ok(None)
        }
    }

    fn queue_key_to_task_id(&self, queue_key: &str) -> String {
        queue_key[self.queue_prefix.len()..].to_owned()
    }

    pub async fn next_task(&mut self) -> Result<Task, TaskStateError> {
        let (mut watcher, mut watch_stream) = self
            .client
            .watch(
                self.queue_prefix.as_bytes(),
                Some(WatchOptions::new().with_prefix()),
            )
            .await?;
        let result = self
            .client
            .get(
                self.queue_prefix.as_bytes(),
                Some(GetOptions::new().with_prefix().with_sort(
                    etcd_client::SortTarget::Create,
                    etcd_client::SortOrder::Ascend,
                )),
            )
            .await?;

        for kv in result.kvs() {
            let task_id = self.queue_key_to_task_id(kv.key_str().unwrap());
            if let Some(task) = self.claim_task(task_id).await? {
                watcher.cancel().await?;
                return Ok(task);
            }
        }

        while let Some(e) = watch_stream.try_next().await? {
            for event in e.events() {
                if event.event_type() == EventType::Put {
                    let task_id = self.queue_key_to_task_id(event.kv().unwrap().key_str().unwrap());
                    if let Some(task) = self.claim_task(task_id).await? {
                        watcher.cancel().await?;
                        return Ok(task);
                    }
                }
            }
        }

        panic!("watch loop ended prematurely");
    }
}

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

impl Task {
    async fn new(
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
        self.verify_status(TaskStatus::Running);
        if self.lease.is_none() {
            panic!("tried to lease a task that was initialized without lease");
        }
        let (mut lease, mut lease_stream) =
            self.client.lease_keep_alive(self.lease.unwrap()).await?;
        lease.keep_alive().await?;
        let _result = lease_stream
            .try_next()
            .await?
            .expect("no keep alive confirmation received");

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

            // clear the interrupt
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
        self.set_status(to).await
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
            .await
    }

    pub async fn finish_error<T: Serialize>(&mut self, error: T) -> Result<(), TaskStateError> {
        self.set_typed_field("error".to_owned(), error)?;
        self.transition_to_status(TaskStatus::Running, TaskStatus::Error)
            .await
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

#[derive(Serialize, Deserialize, Clone)]
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
    #[error(transparent)]
    Etcd(#[from] etcd_client::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Alive(#[from] TaskAliveError),
}
