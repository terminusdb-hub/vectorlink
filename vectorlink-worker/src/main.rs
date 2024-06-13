mod handler;

use clap::Parser;
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

fn test_prometheus() {
    // register metrics to default registry
    let counter = register_counter!("test_counter", "Number of tasks processed").unwrap();
    let gauge = register_gauge!("test_gauge", "Value of metric").unwrap();

    //increment counters
    counter.inc_by(1.0);
    gauge.set(1.0); 
    counter.inc_by(1.0);
    gauge.inc_by(-6.0);

    // gather default registry
    let metric_families = gather();

    // encode MetricFamilys
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    // print to stdout
    println!("{}", String::from_utf8(buffer).unwrap());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (counter, gauge) = test_prometheus();
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
