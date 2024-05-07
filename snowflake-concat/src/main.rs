use std::error::Error;

use clap::Parser;
use itertools::Itertools;

#[derive(Parser, Debug)]
struct Command {
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long)]
    prefix: String,
}

struct SnowflakeName<'a> {
    name: &'a str,
    x: usize,
    y: usize,
    z: usize,
}

impl<'a> SnowflakeName<'a> {
    fn parse(s: &'a str) -> SnowflakeName<'a> {
        let components: Vec<_> = s.rsplitn(4, '_').collect();
        let mut z = components[0];
        z = &z[..".json".len()];
        SnowflakeName {
            name: components[3],
            x: components[2].parse().unwrap(),
            y: components[1].parse().unwrap(),
            z: z.parse().unwrap(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Command::parse();
    let config = aws_config::load_from_env().await;
    let mut client = aws_sdk_s3::Client::new(&config);
    let all_objects = client
        .list_objects()
        .bucket(&args.bucket)
        .prefix(args.prefix)
        .send()
        .await?;

    // object keys are formatted like
    // <name>_<x>_<y>_<z>.json
    // where x, y and z are integers.
    // We first collapse as <name>_<x>_<y>_aggregated.json
    // Then <name>_<x>_aggregated.json
    // Finally <name>_aggregated.json

    let all_objects: Vec<_> = all_objects
        .contents()
        .iter()
        .map(|o| {
            let key = o.key().unwrap();
            let name = SnowflakeName::parse(key);
            (name, key)
        })
        .collect();

    // layer one, group by name, x, y
    let groups = all_objects
        .into_iter()
        .into_group_map_by(|(name, _)| (name.name, name.x, name.y));

    for ((name, x, y), group) in groups {
        let result_key = format!("{name}_{x}_{y}_aggregated.json");
        // run a multipart copy to get this done
    }

    println!("Hello, world!");

    Ok(())
}
