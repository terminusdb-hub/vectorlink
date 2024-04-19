use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::task::block_in_place;
use vectorlink::batch::index_domain;
use vectorlink::openai::Model;
use vectorlink_task::task::{SyncTaskLiveness, TaskHandler, TaskLiveness};

use parallel_hnsw::progress::{Interrupt, ProgressMonitor};

#[derive(Clone, Serialize, Deserialize)]
pub struct BuildIndexRequest {
    domain: String,
    commit: String,
    directory: String,
    model: Model,
    quantized: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum BuildIndexProgress {
    Generate {},
}

#[derive(Serialize, Deserialize)]
pub struct BuildIndexCompletion {
    recall: f32,
}

pub struct VectorlinkTaskHandler;

#[async_trait]
impl TaskHandler for VectorlinkTaskHandler {
    type Init = BuildIndexRequest;

    type Progress = BuildIndexProgress;

    type Complete = BuildIndexCompletion;

    type Error = String;

    async fn initialize(
        _live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        Ok(BuildIndexProgress::Generate {})
    }
    async fn process(
        live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Complete, Self::Error> {
        let key = "fake";
        let request: BuildIndexRequest = live.init().unwrap().unwrap();
        let BuildIndexRequest {
            model,
            directory,
            domain,
            commit,
            quantized,
            ..
        } = request;
        let _state = live.progress().unwrap();
        let live = live.into_sync().unwrap();
        let mut monitor = TaskMonitor(live);
        block_in_place(|| {
            index_domain(
                &key,
                model,
                directory,
                &domain,
                &commit,
                12345,
                quantized,
                &mut monitor,
            )
        })
        .unwrap();

        Ok(BuildIndexCompletion { recall: 0.5 })
    }
}

struct TaskMonitor(SyncTaskLiveness<BuildIndexRequest, BuildIndexProgress>);

impl ProgressMonitor for TaskMonitor {
    fn update(
        &mut self,
        _update: parallel_hnsw::progress::ProgressUpdate,
    ) -> Result<(), parallel_hnsw::progress::Interrupt> {
        let liveness = &mut self.0;
        liveness
            .set_progress(BuildIndexProgress::Generate {})
            .map_err(|_| Interrupt)
    }

    fn keep_alive(&mut self) -> Box<dyn std::any::Any> {
        Box::new(self.0.guarded_keepalive())
    }

    fn alive(&mut self) -> Result<(), Interrupt> {
        self.0.keepalive().map_err(|_| Interrupt)
    }
}
