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
    ResumeAll { task_prefix: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Command::parse();
    let mut client = Client::connect(args.etcd, None).await?;

    match args.command {
        Subcommands::Resume { task_id } => {
            resume::resume(&mut client, &task_id).await?;
        }
        Subcommands::ResumeAll { task_prefix } => {
            resume::resume_all(&mut client, &task_prefix).await?;
        }
    }

    Ok(())
}
