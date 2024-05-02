mod handler;

use clap::Parser;
use vectorlink_task::{queue::Queue, task::TaskHandler};

use crate::handler::LineIndexTaskHandler;

#[derive(Parser, Debug)]
struct Command {
    #[arg(short, long, default_value = "Vec::new()")]
    etcd: Vec<String>,
    #[arg(short, long)]
    identity: Option<String>,
}

fn generate_identity() -> String {
    "line-index-worker".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Command::parse();
    let mut queue = Queue::connect(
        args.etcd,
        None,
        "line-index".to_string(),
        args.identity.unwrap_or_else(generate_identity),
    )
    .await?;
    LineIndexTaskHandler::process_queue(&mut queue).await?;

    unreachable!();
}
