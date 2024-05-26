use std::error::Error;

use clap::Parser;
use etcd_client::{
    Client, Compare, CompareOp, GetOptions, KeyValue, Txn, TxnOp, WatchFilterType, WatchOptions,
};
use futures::TryStreamExt;
use serde_json::json;
use vectorlink_task::{
    key::{
        claim_key, get_increment_key, key_after_prefix, queue_key, task_key_task_id, TASKS_PREFIX,
    },
    task::{TaskData, TaskStatus},
};

#[derive(Parser, Debug)]
pub struct Command {
    #[arg(short, long, default_value = "localhost:2379")]
    etcd: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Command::parse();
    let mut client = Client::connect(args.etcd, None).await?;

    // to start, we have to process any tasks already on the queue.
    let revision = process_existing_tasks(&mut client).await?;

    // then we can start our watch
    process_new_tasks(&mut client, revision + 1).await?;

    Ok(())
}

/// Scan the existing store for any tasks that are ready to be queued, but aren't yet queued.
///
/// Returns the processed revision.
async fn process_existing_tasks(client: &mut Client) -> Result<i64, etcd_client::Error> {
    const LIMIT: i64 = 10000;
    let mut start_key = TASKS_PREFIX.to_vec();
    let end_key = key_after_prefix(TASKS_PREFIX);
    let mut options = GetOptions::new().with_range(end_key).with_limit(LIMIT);
    let mut revision = 0;

    eprintln!("process existing tasks");
    loop {
        let tasks = client.get(&start_key[..], Some(options.clone())).await?;
        if revision == 0 {
            // figure out what revision this is at. we'll keep retrieving from this revision.
            revision = tasks.header().unwrap().revision();
            options = options.with_revision(revision);
        }

        let kvs = tasks.kvs();
        for kv in kvs.iter() {
            try_enqueue_task(client, kv).await?;
        }

        if kvs.len() == LIMIT as usize {
            // there might be more tasks. get those too.
            // We start the next range one key further than the last
            start_key = get_increment_key(kvs.last().unwrap().key());
        } else {
            break;
        }
    }
    eprintln!("done processing existing tasks");
    Ok(revision)
}

async fn process_new_tasks(client: &mut Client, revision: i64) -> Result<(), etcd_client::Error> {
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

    eprintln!("leaving the process loop");
    Ok(())
}

async fn try_enqueue_task(client: &mut Client, kv: &KeyValue) -> Result<(), etcd_client::Error> {
    match serde_json::from_reader::<_, TaskData>(kv.value()).map(|v| v.status) {
        Ok(TaskStatus::Pending) | Ok(TaskStatus::Resuming) | Ok(TaskStatus::Running) => {
            // enqueue - inner enqueue logic ensures that already running tasks aren't re-queued.
            enqueue_task(client, kv.key()).await?;
        }
        Ok(_) => {
            // nothing to do for this task
        }
        Err(e) => {
            eprintln!(
                "unparsable task {:?}: {e}",
                kv.key_str().unwrap_or("with non-string key")
            );
            // yikes, a task on the queue is not good. We should immediately error the task, if it is still there in that shape.
            let _ = client.txn(Txn::new()
                       .when([
                           Compare::value(kv.key(), CompareOp::Equal, kv.value())
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
