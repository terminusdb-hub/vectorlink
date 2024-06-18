mod handler;

use clap::Parser;
use vectorlink_task::{queue::Queue, task::TaskHandler};

use crate::handler::QuantizationHandler;

#[derive(Parser, Debug)]
struct Command {
    #[arg(short, long, default_value = "Vec::new()")]
    etcd: Vec<String>,
    #[arg(short, long, default_value = "vectorlink")]
    service: String,
    #[arg(short, long)]
    identity: Option<String>,
}

fn generate_identity() -> String {
    "quantizer".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Command::parse();
    let mut queue = Queue::connect(
        args.etcd,
        None,
        args.service,
        args.identity.unwrap_or_else(generate_identity),
    )
    .await?;
    QuantizationHandler::process_queue(&mut queue).await?;

    unreachable!();
}
