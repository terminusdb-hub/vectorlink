use async_trait::async_trait;
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};
use serde::{Deserialize, Serialize};
use vectorlink_task::{
    keepalive,
    task::{TaskHandler, TaskLiveness},
};

pub struct LineIndexTaskHandler;

#[derive(Serialize, Deserialize)]
pub struct LineIndexInit {
    bucket: String,
    file_key: String,
    output_key: String,
    chunk_count: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LineIndexProgress {
    upload_id: String,
    parts: Vec<String>,
    file_size: usize,
    chunk_index: usize,
}

#[async_trait]
impl TaskHandler for LineIndexTaskHandler {
    type Init = LineIndexInit;

    type Progress = LineIndexProgress;

    type Complete = ();

    type Error = String;

    async fn initialize(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);
        let init = live.init().unwrap().unwrap();

        let meta = client
            .head_object()
            .bucket(&init.bucket)
            .key(&init.file_key)
            .send()
            .await
            .unwrap();

        let size = meta.content_length.unwrap();

        eprintln!("about to create upload");
        let upload = client
            .create_multipart_upload()
            .bucket(&init.bucket)
            .key(&init.output_key)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let upload_id = upload.upload_id.unwrap();
        eprintln!("upload created: {upload_id:?}");

        Ok(LineIndexProgress {
            upload_id,
            parts: Vec::new(),
            file_size: size as usize,
            chunk_index: 0,
        })
    }
    async fn process(
        mut live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error> {
        let config = keepalive!(live, aws_config::load_from_env().await);
        let client = aws_sdk_s3::Client::new(&config);

        let init = live.init().unwrap().unwrap();
        let mut progress = live.progress().unwrap().unwrap();

        let chunk_count = init.chunk_count.unwrap_or(10000);
        let chunk_size = (progress.file_size + chunk_count - 1) / chunk_count;

        eprintln!(
            "file size: {}, chunk count: {chunk_count}, chunk_size: {chunk_size}",
            progress.file_size
        );

        let start = progress.chunk_index;
        for i in start..chunk_count {
            eprintln!("processing chunk {i}");
            let range = format!("bytes={}-{}", chunk_size * i, chunk_size * (i + 1) - 1);
            eprintln!("range: {range}");
            let dto = keepalive!(
                live,
                client
                    .get_object()
                    .bucket(&init.bucket)
                    .key(&init.file_key)
                    .range(range)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
            );
            eprintln!("retrieved data");
            let mut data = dto.body;

            let mut positions = vec![0]; // first line starts at 0
            while let Some(bytes) =
                keepalive!(live, data.try_next().await.map_err(|e| e.to_string())?)
            {
                positions.extend(
                    bytes
                        .iter()
                        .enumerate()
                        .filter(|(_, b)| **b == b'\n')
                        .map(|(ix, _)| ix + chunk_size * i),
                );
            }
            eprintln!("discovered {} newlines", positions.len());

            let result = {
                let position_bytes = unsafe {
                    std::slice::from_raw_parts(
                        positions[..].as_ptr() as *const u8,
                        std::mem::size_of::<usize>() * positions.len(),
                    )
                };

                let byte_stream = ByteStream::from_static(position_bytes);

                keepalive!(
                    live,
                    client
                        .upload_part()
                        .bucket(&init.bucket)
                        .key(&init.output_key)
                        .upload_id(&progress.upload_id)
                        .part_number(i as i32 + 1)
                        .body(byte_stream)
                        .send()
                        .await
                        .map_err(|e| format!("{e:?}"))?
                )
            };
            eprintln!("sent part {i}");

            let etag = result.e_tag.unwrap();
            progress.parts.push(etag);

            progress.chunk_index += 1;
            live.set_progress(progress.clone())
                .await
                .expect("could not set progress!!");
        }

        eprintln!("done sending parts");

        let parts: Vec<_> = progress
            .parts
            .into_iter()
            .enumerate()
            .map(|(part_num, p)| {
                CompletedPart::builder()
                    .e_tag(p)
                    .part_number(part_num as i32 + 1)
                    .build()
            })
            .collect();
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        // finalizing time
        let _result = keepalive!(
            live,
            client
                .complete_multipart_upload()
                .bucket(&init.bucket)
                .key(&init.output_key)
                .upload_id(&progress.upload_id)
                .multipart_upload(completed)
                .send()
                .await
                .map_err(|e| format!("{e:?}"))?
        );

        eprintln!("finalized!");

        Ok(())
    }
}
