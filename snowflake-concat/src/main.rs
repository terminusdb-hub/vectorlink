use std::error::Error;

use aws_sdk_s3::{
    config::StalledStreamProtectionConfig,
    types::{CompletedMultipartUpload, CompletedPart},
};
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
    fn try_parse(s: &'a str) -> Option<SnowflakeName<'a>> {
        let components: Vec<_> = s.rsplitn(4, '_').collect();
        let mut z = components[0];
        z = &z[..(z.len() - ".json".len())];
        Some(SnowflakeName {
            name: components[3],
            x: components[2].parse().ok()?,
            y: components[1].parse().ok()?,
            z: z.parse().ok()?,
        })
    }
}

async fn multipart_concat<S: AsRef<str>, I: Iterator<Item = S> + ExactSizeIterator>(
    client: &mut aws_sdk_s3::Client,
    bucket: &str,
    files: I,
    concatenated: &str,
) -> Result<(), aws_sdk_s3::Error> {
    /*
    eprintln!("bucket: {bucket}");
    eprintln!("this would have created {concatenated}");
    eprintln!("inputs:");
    for f in files {
        eprintln!(" {}", f.as_ref());
    }
    */
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(concatenated)
        .send()
        .await?
        .upload_id
        .unwrap();

    eprintln!("upload id: {upload_id}");

    let mut parts = Vec::with_capacity(files.len());
    for (ix, file) in files.enumerate() {
        let part_num = (ix + 1) as i32;
        let e_tag = client
            .upload_part_copy()
            .part_number(part_num)
            .upload_id(&upload_id)
            .bucket(bucket)
            .key(concatenated)
            .copy_source(format!("{bucket}/{}", file.as_ref()))
            .send()
            .await?
            .copy_part_result()
            .unwrap()
            .e_tag()
            .unwrap()
            .to_string();
        eprintln!("{e_tag}");

        parts.push(
            CompletedPart::builder()
                .part_number(part_num)
                .e_tag(e_tag)
                .build(),
        )
    }

    client
        .complete_multipart_upload()
        .upload_id(&upload_id)
        .bucket(bucket)
        .key(concatenated)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await?;
    eprintln!("completed");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Command::parse();
    let config = aws_config::load_from_env().await;
    let config = config
        .into_builder()
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .build();
    let mut client = aws_sdk_s3::Client::new(&config);
    let mut all_objects = Vec::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let some_objects = client
            .list_objects_v2()
            .bucket(&args.bucket)
            .prefix(&args.prefix)
            .set_continuation_token(continuation_token)
            .send()
            .await?;

        let new_continuation_token = some_objects
            .next_continuation_token()
            .map(|s| s.to_string());

        all_objects.extend(some_objects.contents.unwrap());

        if new_continuation_token.is_none() {
            break;
        }

        continuation_token = new_continuation_token;
    }

    // object keys are formatted like
    // <name>_<x>_<y>_<z>.json
    // where x, y and z are integers.
    // We first collapse as <name>_<x>_<y>_aggregated.json
    // Then <name>_<x>_aggregated.json
    // Finally <name>_aggregated.json

    let all_objects: Vec<((&str, usize, usize), &str)> = all_objects
        .iter()
        .filter_map(|o| {
            let key = o.key().unwrap();
            if !key.ends_with("_aggregated.json") {
                None
            } else {
                let parsed: Vec<_> = key.rsplitn(4, '_').collect();
                Some((
                    (parsed[3], parsed[2].parse().ok()?, parsed[1].parse().ok()?),
                    key,
                ))
            }
            //let name = SnowflakeName::try_parse(key)?;
            //Some((name, key))
        })
        .collect();

    eprintln!("found {} objects", all_objects.len());

    // layer one, group by name, x, y
    let groups = all_objects
        .into_iter()
        .into_group_map_by(|(name, _)| (name.0, name.1));

    let mut aggregated_keys = Vec::new();
    for ((name, x), mut group) in groups {
        group.sort_by_key(|v| v.0 .1);
        let result_key = format!("{name}_{x}_aggregated.json");
        eprintln!("concatenating {result_key}");
        // run a multipart copy to get this done
        multipart_concat(
            &mut client,
            &args.bucket,
            group.iter().map(|g| g.1),
            &result_key,
        )
        .await?;

        aggregated_keys.push(((name, x), result_key));

        eprintln!("concatenated");
    }

    // final layer, group by name
    let groups = aggregated_keys
        .into_iter()
        .into_group_map_by(|(name, _)| name.0);

    for (name, mut group) in groups {
        group.sort_by_key(|v| v.0 .1);
        let result_key = format!("{name}_aggregated.json");
        eprintln!("concatenating {result_key}");
        // run a multipart copy to get this done
        multipart_concat(
            &mut client,
            &args.bucket,
            group.iter().map(|g| &g.1),
            &result_key,
        )
        .await?;

        eprintln!("concatenated");
    }

    println!("Done");

    Ok(())
}
