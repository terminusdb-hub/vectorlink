mod handler;

use clap::Parser;
use vectorlink_task::{queue::Queue, task::TaskHandler};

use crate::handler::VectorlinkTaskHandler;

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
    "worker".to_string()
}

fn wait() -> (){
    use std::io::{self, Write};
    println!("Press enter to continue...");
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
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

    VectorlinkTaskHandler::process_queue(&mut queue).await?;

    unreachable!();
}
