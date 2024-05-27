use etcd_client::{Client, Compare, CompareOp, KeyValue, Txn, TxnOp, TxnOpResponse};
use vectorlink_task::{
    key::{task_key, task_key_task_id},
    task::{TaskData, TaskStatus},
};

pub async fn wake_up_parent(
    client: &mut Client,
    task: &KeyValue,
    task_data: TaskData,
) -> Result<(), etcd_client::Error> {
    if let Some(parent) = task_data.parent.as_ref() {
        // we might have to wake up this parent.
        let parent_task_key = task_key(parent.as_bytes());
        let response = client.get(&parent_task_key[..], None).await?;
        if response.kvs().is_empty() {
            // parent doesn't exist. that is weird, but let's just ignore it.
            return Ok(());
        }

        // but if it does exist, we're gonna want to wake it up now.
        let kv = &response.kvs()[0];
        let task_id = task_key_task_id(task.key());
        match serde_json::from_reader::<_, TaskData>(kv.value()) {
            Ok(parent_task) => {
                if parent_task.status == TaskStatus::Waiting
                    && parent_task
                        .waiting
                        .iter()
                        .flatten()
                        .any(|waiting_for| waiting_for.as_bytes() == task_id)
                {
                    wake_up_waiting_task(client, kv, parent_task).await?;
                }
            }
            Err(_) => {
                // ignore any parse errors. The task will
                // automatically go to the error state when it is
                // processed for the first time, and it won't be
                // capable of being woken up anyway.
            }
        }
    }
    Ok(())
}

pub async fn try_resume_waiting(
    client: &mut Client,
    task: &KeyValue,
    task_data: TaskData,
) -> Result<(), etcd_client::Error> {
    if task_data.status != TaskStatus::Waiting {
        // no need to do any
        return Ok(());
    }

    if let Some(waiting) = task_data.waiting.as_ref() {
        // retrieve all tasks
        let ops: Vec<_> = waiting
            .iter()
            .map(|w| TxnOp::get(task_key(w.as_bytes()), None))
            .collect();
        if ops.is_empty() {
            // waiting for nothing? just wake up already!
            wake_up_waiting_task(client, task, task_data).await?;
            return Ok(());
        }

        let result = client.txn(Txn::new().and_then(ops)).await?;
        for response in result.op_responses() {
            if let TxnOpResponse::Get(r) = response {
                if r.kvs().is_empty() {
                    // task not found. hopefully it'll be created later.
                    continue;
                }

                let wait_kv = &r.kvs()[0];
                // we now have a task to check. if it is in a complete state, we can resume.
                if let Ok(wait_data) = serde_json::from_reader::<_, TaskData>(wait_kv.value()) {
                    if wait_data.status.is_final_state() {
                        // this task is completed! wake up time
                        wake_up_waiting_task(client, task, task_data).await?;
                        // no need to wake this up twice, so let's bail.
                        break;
                    }
                }
            }
        }
    } else {
        // waiting for nothing? just wake up already!
        wake_up_waiting_task(client, task, task_data).await?;
    }
    Ok(())
}

pub async fn wake_up_waiting_task(
    client: &mut Client,
    task: &KeyValue,
    mut task_data: TaskData,
) -> Result<(), etcd_client::Error> {
    task_data.status = TaskStatus::Resuming;
    // wake up!
    // but only if the task hasn't changed. If it did
    // change, the watch will catch it.
    let result = client
        .txn(
            Txn::new()
                .when([Compare::version(
                    task.key(),
                    CompareOp::Equal,
                    task.version(),
                )])
                .and_then([TxnOp::put(
                    task.key(),
                    serde_json::to_vec(&task_data).unwrap(),
                    None,
                )]),
        )
        .await?;

    if result.succeeded() {
        eprintln!(
            "woke up waiting task {}",
            String::from_utf8_lossy(task.key())
        );
    }

    Ok(())
}
