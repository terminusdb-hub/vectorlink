use std::process;

use etcd_client::{Client, Compare, Txn, TxnOp};
use vectorlink_task::task::{TaskData, TaskStatus};

pub async fn resume(
    mut client: Client,
    task_id: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let key = format!("/services/tasks/{task_id}").into_bytes();
    let vals = client.get(key.clone(), None).await?;

    if vals.kvs().is_empty() {
        eprintln!("task not found: {task_id}");
        return Ok(false);
    }
    let kv = &vals.kvs()[0];

    let mut task_data: TaskData =
        serde_json::from_reader(kv.value()).expect("task was not valid task data");
    if task_data.status != TaskStatus::Error {
        eprintln!(
            "task status is {:?}, so no resume happened.",
            task_data.status
        );
        return Ok(false);
    }

    task_data.status = TaskStatus::Resuming;
    task_data.other_fields.remove("error");
    let task_data_bytes: Vec<u8> = serde_json::to_vec(&task_data).unwrap();

    let current_version = kv.version();

    let result = client
        .txn(
            Txn::new()
                .when([Compare::version(
                    key.clone(),
                    etcd_client::CompareOp::Equal,
                    current_version,
                )])
                .and_then([TxnOp::put(key.clone(), task_data_bytes, None)]),
        )
        .await?;

    if result.succeeded() {
        eprintln!("resumed task {task_id}.");
        Ok(true)
    } else {
        eprintln!("task {task_id} changed while processing.");
        Ok(false)
    }
}
