use crate::{
    orphan::resume_if_unclaimed,
    wait::{try_resume_waiting, wake_up_parent},
};
use etcd_client::{
    Client, Compare, CompareOp, KeyValue, Txn, TxnOp, WatchFilterType, WatchOptions,
};
use futures::TryStreamExt;
use serde_json::json;
use vectorlink_task::{
    key::{claim_key, queue_key, task_key_task_id, TASKS_PREFIX},
    task::{TaskData, TaskStatus},
};

pub async fn process_task_updates(
    client: &mut Client,
    revision: i64,
) -> Result<(), etcd_client::Error> {
    eprintln!("start watching for new tasks");
    let (_watcher, mut watch_stream) = client
        .watch(
            TASKS_PREFIX,
            Some(
                WatchOptions::new()
                    .with_prefix()
                    .with_start_revision(revision)
                    .with_filters([WatchFilterType::NoDelete])
                    .with_fragment(),
            ),
        )
        .await?;

    while let Some(response) = watch_stream.try_next().await? {
        if response.canceled() {
            break;
        }
        for event in response.events() {
            let kv = event.kv();
            if kv.is_none() {
                // weird, but whatever
                continue;
            }
            let kv = kv.unwrap();

            try_enqueue_task(client, kv).await?;
        }
    }

    eprintln!("leaving the task process loop");
    Ok(())
}

pub async fn try_enqueue_task(
    client: &mut Client,
    kv: &KeyValue,
) -> Result<(), etcd_client::Error> {
    match serde_json::from_reader::<_, TaskData>(kv.value()) {
        Ok(parsed) => {
            match parsed.status {
                TaskStatus::Pending | TaskStatus::Resuming => {
                    // enqueue - inner enqueue logic ensures that already running tasks aren't re-queued.
                    enqueue_task(client, kv.key()).await?;
                }
                TaskStatus::Running => {
                    // resume if no claim
                    resume_if_unclaimed(client, kv, parsed).await?;
                }
                TaskStatus::Complete | TaskStatus::Error | TaskStatus::Canceled => {
                    // resume parent, if it is waiting for us
                    wake_up_parent(client, kv, parsed).await?;
                    // TODO we also have to cancel any remaining children
                }
                TaskStatus::Waiting => {
                    // see if we can resume
                    try_resume_waiting(client, kv, parsed).await?;
                }
                _ => {
                    // nothing to do for this task
                }
            }
        }
        Err(e) => {
            eprintln!(
                "unparsable task {:?}: {e}",
                kv.key_str().unwrap_or("with non-string key")
            );
            // yikes, a task on the queue is not good. We should immediately error the task, if it is still at the same version.
            let _ = client.txn(Txn::new()
                       .when([
                           Compare::version(kv.key(), CompareOp::Equal, kv.version())
                       ])
                       .and_then([
                           TxnOp::put(kv.key(), serde_json::to_vec(&json!({"status":"error","error":format!("unparsable task: {e}"), "original": String::from_utf8_lossy(kv.value())})).unwrap(), None)
                       ])
                       ).await?;
        }
    }

    Ok(())
}

/// Enqueue a task, if it isn't already enqueued.
async fn enqueue_task(client: &mut Client, task_key: &[u8]) -> Result<(), etcd_client::Error> {
    let task_id = task_key_task_id(task_key);
    let claim = claim_key(task_id);
    let queue = queue_key(task_id);
    let result = client
        .txn(
            Txn::new()
                .when([
                    Compare::version(claim, CompareOp::Equal, 0),
                    Compare::version(queue.clone(), CompareOp::Equal, 0),
                ])
                .and_then([TxnOp::put(queue, b"", None)]),
        )
        .await?;

    if result.succeeded() {
        eprintln!("enqueue {}", String::from_utf8_lossy(task_key));
    }

    Ok(())
}
