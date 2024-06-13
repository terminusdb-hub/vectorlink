mod handler;

use clap::Parser;
use vectorlink_task::{queue::Queue, task::TaskHandler};
use prometheus_exporter::{self, prometheus::register_counter};

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

fn init_prometheus() {
    let counter = register_counter!("test_counter", "Number of tasks processed").unwrap();
    let gauge = register_gauge!("test_gauge", "Value of metric").unwrap();
    counter.inc_by(1.0);
    gauge.set(1.0);
    (counter, gauge)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (counter, gauge) = init_prometheus();
    counter.inc_by(1.0);
    gauge.inc_by(-6.0);
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
