use crate::task::try_enqueue_task;
use etcd_client::{Client, GetOptions};
use vectorlink_task::key::{get_increment_key, key_after_prefix, TASKS_PREFIX};

/// Scan the existing store for any tasks that are ready to be queued, but aren't yet queued.
///
/// Returns the processed revision.
pub async fn process_existing_tasks(client: &mut Client) -> Result<i64, etcd_client::Error> {
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
