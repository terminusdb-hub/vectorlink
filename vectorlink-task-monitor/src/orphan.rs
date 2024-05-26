use etcd_client::{
    Client, Compare, CompareOp, GetOptions, KeyValue, Txn, TxnOp, WatchFilterType, WatchOptions,
};
use futures::TryStreamExt;
use vectorlink_task::{
    key::{claim_key, claim_key_task_id, interrupt_key, task_key, task_key_task_id, CLAIMS_PREFIX},
    task::{TaskData, TaskStatus},
};

use crate::task::try_enqueue_task;

pub async fn process_new_orphans(
    client: &mut Client,
    revision: i64,
) -> Result<(), etcd_client::Error> {
    eprintln!("start watching for orphaned tasks");
    let (_watcher, mut watch_stream) = client
        .watch(
            CLAIMS_PREFIX,
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(revision)
                    .with_filters([WatchFilterType::NoPut])
                    .with_fragment(),
            ),
        )
        .await?;

    while let Some(response) = watch_stream.try_next().await? {
        if response.canceled() {
            break;
        }
        let current_revision = response.header().expect("no header").revision();
        for event in response.events() {
            let kv = event.kv();
            if kv.is_none() {
                // weird, but whatever
                continue;
            }
            let kv = kv.unwrap();

            let task_id = claim_key_task_id(kv.key());
            let task_key = task_key(task_id);

            let task_kv = client
                .get(
                    task_key,
                    Some(GetOptions::new().with_revision(current_revision)),
                )
                .await?
                .take_kvs()
                .into_iter()
                .next()
                .unwrap();

            try_enqueue_task(client, &task_kv).await?;
        }
    }

    eprintln!("leaving the orphan process loop");
    Ok(())
}

pub async fn resume_unclaimed(
    client: &mut Client,
    kv: &KeyValue,
    mut task_data: TaskData,
) -> Result<(), etcd_client::Error> {
    task_data.status = TaskStatus::Resuming;

    let task_id = task_key_task_id(kv.key());
    let interrupt_key = interrupt_key(task_id);
    let claim_key = claim_key(task_id);

    let serialized = serde_json::to_vec(&task_data).expect("serialization of task failed");

    let result = client
        .txn(
            Txn::new()
                .when([
                    Compare::version(kv.key(), CompareOp::Equal, kv.version()),
                    Compare::version(claim_key, CompareOp::Equal, 0),
                ])
                .and_then([
                    TxnOp::put(kv.key(), serialized, None),
                    TxnOp::delete(interrupt_key, None),
                ]),
        )
        .await?;

    if result.succeeded() {
        eprintln!("resume {}", String::from_utf8_lossy(kv.key()));
    }

    Ok(())
}
