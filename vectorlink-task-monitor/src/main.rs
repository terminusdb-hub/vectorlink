mod init;
mod orphan;
mod task;
mod wait;

use std::error::Error;

use clap::Parser;
use etcd_client::Client;

use crate::{init::process_existing_tasks, orphan::process_orphans, task::process_task_updates};

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

    // then we can start our watches. The select will ensure that if
    // any of them finish, the other tasks will be canceled.
    let mut task_client = client.clone();
    tokio::select! {
        _ = process_task_updates(&mut task_client, revision + 1) => {},
        _ = process_orphans(&mut client, revision+1) => {}
    }

    Ok(())
}
