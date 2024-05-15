use std::{error::Error, sync::Arc};

use aws_sdk_s3::{
    config::StalledStreamProtectionConfig,
    types::{CompletedMultipartUpload, CompletedPart},
};
use clap::Parser;
use itertools::Itertools;
use leaky_bucket::RateLimiter;

#[derive(Parser, Debug)]
struct Command {
    #[arg(long)]
    source_bucket: String,
    #[arg(long)]
    destination_bucket: String,
    #[arg(long)]
    source_prefix: String,
    #[arg(long)]
    destination_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct SnowflakeName {
    name: String,
    x: usize,
    y: usize,
    z: usize,
}

impl SnowflakeName {
    fn try_parse(s: &str) -> Option<SnowflakeName> {
        let components: Vec<_> = s.rsplitn(4, '_').collect();
        let mut z = components[0];
        z = &z[..(z.len() - ".json".len())];
        Some(SnowflakeName {
            name: components[3].to_string(),
            x: components[2].parse().ok()?,
            y: components[1].parse().ok()?,
            z: z.parse().ok()?,
        })
    }
}

async fn multipart_concat(
    client: Arc<aws_sdk_s3::Client>,
    source_bucket: String,
    target_bucket: String,
    files: Vec<String>,
    concatenated: String,
    check_source_size: bool,
) -> Result<(), aws_sdk_s3::Error> {
    /*
    eprintln!("source bucket: {source_bucket}");
    eprintln!("target bucket: {target_bucket}");
    eprintln!("this would have created {concatenated}");
    eprintln!("inputs:");
    for f in files {
        eprintln!(" {}", f);
    }
    */
    let upload_id = client
        .create_multipart_upload()
        .bucket(&target_bucket)
        .key(&concatenated)
        .send()
        .await?
        .upload_id
        .unwrap();

    eprintln!("upload id: {upload_id}");

    let mut uploads = Vec::with_capacity(files.len());
    let mut part_num: i32 = 1;
    for file in files {
        eprintln!("{file}");
        if check_source_size {
            // we're gonna check the source file size to make sure it's smaller than 5GB.
            // if it is larger ,we'll have to do multiple upload_part_copy invocations.
            let length = client
                .head_object()
                .bucket(&source_bucket)
                .key(&file)
                .send()
                .await?
                .content_length
                .unwrap() as usize;
            let split_count = (length + (5 << 30) - 1) / (5 << 30);
            let segment_size = (length + split_count - 1) / split_count;
            for split_index in 0..split_count {
                let start = split_index * segment_size;
                let end = usize::min((split_index + 1) * segment_size - 1, length - 1);
                eprintln!(" segment {start} {end}");
                let task = tokio::spawn(
                    client
                        .upload_part_copy()
                        .part_number(part_num)
                        .upload_id(&upload_id)
                        .bucket(&target_bucket)
                        .key(&concatenated)
                        .copy_source(format!("{source_bucket}/{}", file))
                        .copy_source_range(format!("bytes={}-{}", start, end))
                        .send(),
                );
                uploads.push((part_num, task));
                part_num += 1;
            }
        } else {
            let task = tokio::spawn(
                client
                    .upload_part_copy()
                    .part_number(part_num)
                    .upload_id(&upload_id)
                    .bucket(&target_bucket)
                    .key(&concatenated)
                    .copy_source(format!("{source_bucket}/{}", file))
                    .send(),
            );
            uploads.push((part_num, task));
            part_num += 1;
        }
    }
    let mut parts = Vec::with_capacity(uploads.len());
    for (part_num, upload) in uploads {
        let e_tag = upload
            .await
            .expect("task panicked")?
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

    eprintln!("finalizing..");
    client
        .complete_multipart_upload()
        .upload_id(&upload_id)
        .bucket(target_bucket)
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
    let client = Arc::new(aws_sdk_s3::Client::new(&config));
    let mut all_objects = Vec::new();
    let mut continuation_token: Option<String> = None;
    loop {
        let some_objects = client
            .list_objects_v2()
            .bucket(&args.source_bucket)
            .prefix(&args.source_prefix)
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

    let all_objects: Vec<_> = all_objects
        .into_iter()
        .filter_map(|o| {
            let key = o.key.unwrap();
            let name = SnowflakeName::try_parse(&key)?;

            Some((name, key))
        })
        .collect();

    eprintln!("found {} objects", all_objects.len());

    let rate_limiter = Arc::new(
        RateLimiter::builder()
            .fair(false)
            .initial(1000)
            .max(1000)
            .refill(10)
            .build(),
    );

    // layer one, group by name, x, y
    let groups = all_objects
        .into_iter()
        .into_group_map_by(|(SnowflakeName { name, x, y, .. }, _)| (name.clone(), *x, *y));

    let mut aggregated_key_tasks = Vec::new();
    let mut aggregated_keys = Vec::new();
    for ((name, x, y), mut group) in groups {
        group.sort_by_key(|v| v.0.clone());
        rate_limiter.acquire(group.len()).await;
        let result_key = format!("{name}_{x}_{y}_aggregated.json");
        eprintln!("concatenating {result_key}");
        aggregated_key_tasks.push(tokio::spawn(multipart_concat(
            client.clone(),
            args.source_bucket.clone(),
            args.destination_bucket.clone(),
            group.into_iter().map(|g| g.1).collect(),
            result_key.clone(),
            false,
        )));
        aggregated_keys.push(((name, x, y), result_key));
    }

    for task in aggregated_key_tasks {
        task.await??;
        eprintln!("concatenated");
    }

    // layer two, group by name, x
    println!("layer 2");
    let groups = aggregated_keys
        .into_iter()
        .into_group_map_by(|((name, x, _y), _)| (name.clone(), *x));
    let mut aggregated_key_tasks = Vec::new();
    let mut aggregated_keys = Vec::new();
    for ((name, x), mut group) in groups {
        group.sort_by_key(|v| v.0.clone());
        rate_limiter.acquire(group.len()).await;
        let result_key = format!("{name}_{x}_aggregated.json");
        eprintln!("concatenating {result_key}");
        aggregated_key_tasks.push(tokio::spawn(multipart_concat(
            client.clone(),
            args.destination_bucket.clone(),
            args.destination_bucket.clone(),
            group.into_iter().map(|g| g.1).collect(),
            result_key.clone(),
            true,
        )));
        aggregated_keys.push(((name, x), result_key));
    }

    for task in aggregated_key_tasks {
        task.await??;
        eprintln!("concatenated");
    }

    println!("layer 3");
    let groups = aggregated_keys
        .into_iter()
        .into_group_map_by(|((name, _x), _)| name.clone());
    let mut aggregated_key_tasks = Vec::new();
    let mut aggregated_keys = Vec::new();
    for (name, mut group) in groups {
        group.sort_by_key(|v| v.0.clone());
        rate_limiter.acquire(group.len()).await;
        let result_key = format!("{name}_aggregated.json");
        eprintln!("concatenating {result_key}");
        aggregated_key_tasks.push(tokio::spawn(multipart_concat(
            client.clone(),
            args.destination_bucket.clone(),
            args.destination_bucket.clone(),
            group.into_iter().map(|g| g.1).collect(),
            result_key.clone(),
            true,
        )));
        aggregated_keys.push((name, result_key));
    }

    for task in aggregated_key_tasks {
        task.await??;
        eprintln!("concatenated");
    }

    println!("Done");

    Ok(())
}
