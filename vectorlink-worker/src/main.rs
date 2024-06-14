mod handler;

use clap::Parser;
use prometheus::core::{AtomicF64, GenericCounter};
use vectorlink_task::{queue::Queue, task::TaskHandler};
use prometheus_exporter::{
    self, 
    prometheus::{register_counter, register_gauge, TextEncoder, gather},
};

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

fn start_prometheus_exporter() -> () {
    let binding = "127.0.0.1:9184".parse().unwrap();
    prometheus_exporter::start(binding).unwrap();
}

fn register_metrics() -> (GenericCounter<AtomicF64>, GenericCounter<AtomicF64>, GenericCounter<AtomicF64>) {
    let worker_started_counter = register_counter!("worker_started_counter", "Number of workers started").unwrap();
    let successful_connection_counter = register_counter!("successful_connection_counter", "Number of successful connections to etcd").unwrap();
    let successful_task_counter = register_counter!("successful_task_counter", "Number of successful tasks processed").unwrap();

    (worker_started_counter, successful_connection_counter, successful_task_counter)
}

fn wait() -> (){
    use std::io::{self, Write};
    println!("Press enter to continue...");
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    start_prometheus_exporter();
    
    let (worker_started_counter, successful_connection_counter, successful_task_counter) = register_metrics();
    let metric_families = gather();
    let encoder = TextEncoder::new();
    encoder.encode_to_string(&metric_families).unwrap(); // not sure yet if this is necessary

    worker_started_counter.inc();

    let args = Command::parse();
    let mut queue = Queue::connect(
        args.etcd,
        None,
        args.service,
        args.identity.unwrap_or_else(generate_identity),
    )
    .await?;
    successful_connection_counter.inc();

    VectorlinkTaskHandler::process_queue(&mut queue).await?;
    successful_task_counter.inc();

    // wait(); // for testing; you can see the metrics at http://localhost:9184/metrics
    unreachable!();
}
