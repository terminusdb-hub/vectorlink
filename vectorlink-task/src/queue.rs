use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, PutOptions, Txn, TxnOp,
    WatchOptions,
};
use tokio_stream::StreamExt;

use crate::task::{Task, TaskStateError};

pub struct Queue {
    pub(crate) client: Client,
    #[allow(unused)]
    pub(crate) service_name: String,
    pub(crate) identity: String,
    pub(crate) queue_prefix: String,
    pub(crate) tasks_prefix: String,
    pub(crate) claims_prefix: String,
    pub(crate) interrupt_prefix: String,
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
