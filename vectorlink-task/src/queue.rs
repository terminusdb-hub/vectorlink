use crate::key::*;
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
    pub(crate) queue_prefix: Vec<u8>,
    pub(crate) tasks_prefix: Vec<u8>,
    pub(crate) claims_prefix: Vec<u8>,
    pub(crate) interrupt_prefix: Vec<u8>,
}

impl Queue {
    pub async fn connect<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        options: Option<ConnectOptions>,
        service_name: String,
        identity: String,
    ) -> Result<Self, etcd_client::Error> {
        let client = Client::connect(endpoints, options).await?;

        let queue_prefix = concat_bytes(QUEUE_PREFIX, service_name.as_bytes());
        let tasks_prefix = concat_bytes(TASKS_PREFIX, service_name.as_bytes());
        let claims_prefix = concat_bytes(CLAIMS_PREFIX, service_name.as_bytes());
        let interrupt_prefix = concat_bytes(INTERRUPT_PREFIX, service_name.as_bytes());
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
        let queue_key = concat_bytes(&self.queue_prefix, task_id.as_bytes());
        let claim_key = concat_bytes(&self.claims_prefix, task_id.as_bytes());

        let lease = self.client.lease_grant(10, None).await?;
        let result = self
            .client
            .txn(
                Txn::new()
                    .when([Compare::version(&claim_key[..], CompareOp::Equal, 0)])
                    .and_then([
                        TxnOp::delete(&queue_key[..], None),
                        TxnOp::put(
                            &claim_key[..],
                            self.identity.as_bytes(),
                            Some(PutOptions::new().with_lease(lease.id())),
                        ),
                    ])
                    .or_else([TxnOp::delete(&queue_key[..], None)]),
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
        let mut start_key = self.queue_prefix.to_vec();
        let end_key = key_after_prefix(&self.queue_prefix);
        let mut revision = 0;
        loop {
            let result = self
                .client
                .get(
                    &start_key[..],
                    Some(
                        GetOptions::new()
                            .with_range(&end_key[..])
                            .with_sort(
                                etcd_client::SortTarget::Create,
                                etcd_client::SortOrder::Ascend,
                            )
                            .with_limit(100),
                    ),
                )
                .await?;

            if revision == 0 {
                revision = result.header().expect("no header").revision();
            }

            for kv in result.kvs() {
                let task_id = self.queue_key_to_task_id(kv.key_str().unwrap());
                if let Some(task) = self.claim_task(task_id).await? {
                    return Ok(task);
                }
            }

            if !result.more() {
                break;
            }

            // we need to look at more results. set start key appropriately
            start_key = get_increment_key(result.kvs().last().expect("kvs empty??").key());
        }

        // after having processed all keys, we still didn't find a
        // potential task. Let's just wait for one to pop up.
        let (mut watcher, mut watch_stream) = self
            .client
            .watch(
                &self.queue_prefix[..],
                Some(
                    WatchOptions::new()
                        .with_prefix()
                        .with_fragment()
                        .with_start_revision(revision),
                ),
            )
            .await?;

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
