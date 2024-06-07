mod resume;

use clap::{Parser, Subcommand};
use etcd_client::Client;

#[derive(Debug, Parser)]
struct Command {
    #[arg(short, long, default_value = "Vec::new()")]
    etcd: Vec<String>,
    #[command(subcommand)]
    command: Subcommands,
}

#[derive(Debug, Clone, Subcommand)]
enum Subcommands {
    Resume { task_id: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Command::parse();
    let client = Client::connect(args.etcd, None).await?;

    match args.command {
        Subcommands::Resume { task_id } => {
            resume::resume(client, task_id).await?;
        }
    }

    Ok(())
}
