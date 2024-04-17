use std::{thread::sleep, time::Duration};

use async_trait::async_trait;
use vectorlink_task::{
    queue::Queue,
    task::{TaskHandler, TaskLiveness},
};

struct MyCoolWorker;

#[async_trait]
impl TaskHandler for MyCoolWorker {
    type Init = ();
    type Progress = u64;
    type Complete = String;
    type Error = String;

    async fn initialize(
        _live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        Ok(0)
    }

    async fn process(
        mut live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error> {
        let start = live
            .progress()
            .map_err(|e| e.to_string())
            .expect("serialization error")
            .unwrap_or(0);
        for i in start..=100 {
            println!("{i}");
            live.set_progress(i).await.expect("could not set progress!");
            sleep(Duration::from_secs(1));
        }

        Ok("all done".to_string())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut queue = Queue::connect(
        ["localhost:2379"],
        None,
        "vectorlink".to_owned(),
        "worker".to_owned(),
    )
    .await?;

    MyCoolWorker::process_queue(&mut queue).await?;

    unreachable!();
}
