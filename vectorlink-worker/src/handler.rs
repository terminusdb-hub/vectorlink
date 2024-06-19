use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::task::block_in_place;
use vectorlink::batch::index_domain;
use vectorlink::openai::Model;
use vectorlink_task::task::{SyncTaskLiveness, TaskHandler, TaskLiveness};

use parallel_hnsw::progress::{Interrupt, LayerStatistics, ProgressMonitor};

#[derive(Clone, Serialize, Deserialize)]
pub struct BuildIndexRequest {
    domain: String,
    commit: String,
    directory: String,
    model: Model,
    quantized: bool,
}

// progress is just a json value for now

#[derive(Serialize, Deserialize)]
pub struct BuildIndexCompletion {
    recall: f32,
}

pub struct VectorlinkTaskHandler;

#[derive(Serialize, Deserialize, Clone)]
pub struct IndexProgress {
    state: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    centroid_state: Option<Value>,
    statistics: HashMap<usize, LayerStatistics>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    centroid_statistics: HashMap<usize, LayerStatistics>,
}

#[async_trait]
impl TaskHandler for VectorlinkTaskHandler {
    type Init = BuildIndexRequest;

    // TODO: actual progress should not be an arbitrary json object but a meaningful serializable state object.
    type Progress = IndexProgress;

    type Complete = BuildIndexCompletion;

    type Error = String;

    async fn initialize(
        _live: TaskLiveness<Self::Init, Self::Progress>,
    ) -> Result<Self::Progress, Self::Error> {
        Ok(IndexProgress {
            state: json!({}),
            statistics: HashMap::new(),
            centroid_state: None,
            centroid_statistics: HashMap::new(),
        })
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

        // TODO: this should obviously not be a fake value
        Ok(BuildIndexCompletion { recall: 0.5 })
    }
}

struct TaskMonitor(SyncTaskLiveness<BuildIndexRequest, IndexProgress>);

impl ProgressMonitor for TaskMonitor {
    fn update(
        &mut self,
        update: parallel_hnsw::progress::ProgressUpdate,
    ) -> Result<(), parallel_hnsw::progress::Interrupt> {
        let liveness = &mut self.0;
        let mut progress = liveness.progress().unwrap().clone();
        progress.state = update.state;
        liveness.set_progress(progress).map_err(|_| Interrupt)
    }

    fn set_layer_statistics(
        &mut self,
        layer: usize,
        statistics: parallel_hnsw::progress::LayerStatistics,
    ) -> Result<(), Interrupt> {
        let liveness = &mut self.0;
        let mut progress = liveness.progress().unwrap().clone();
        progress.statistics.insert(layer, statistics);
        liveness.set_progress(progress).map_err(|_| Interrupt)
    }

    fn keep_alive(&mut self) -> Box<dyn std::any::Any> {
        Box::new(self.0.guarded_keepalive().expect("lease not live!"))
    }

    fn alive(&mut self) -> Result<(), Interrupt> {
        self.0.keepalive().map_err(|_| Interrupt)
    }

    fn centroid_update(
        &mut self,
        update: parallel_hnsw::progress::ProgressUpdate,
    ) -> Result<(), Interrupt> {
        let liveness = &mut self.0;
        let mut progress = liveness.progress().unwrap().clone();
        progress.centroid_state = Some(update.state);
        liveness.set_progress(progress).map_err(|_| Interrupt)
    }

    fn set_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
        statistics: LayerStatistics,
    ) -> Result<(), Interrupt> {
        let liveness = &mut self.0;
        let mut progress = liveness.progress().unwrap().clone();
        progress
            .centroid_statistics
            .insert(layer_from_top, statistics);
        liveness.set_progress(progress).map_err(|_| Interrupt)
    }

    fn get_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        Ok(self
            .0
            .progress()
            .and_then(|p| p.centroid_statistics.get(&layer_from_top))
            .copied())
    }

    fn invalidate_layer_statistics(&mut self, layer_from_top: usize) -> Result<(), Interrupt> {
        let progress = self.0.progress();
        if progress.is_none() {
            return Ok(());
        }
        let mut progress = progress.unwrap().clone();
        progress.statistics.remove(&layer_from_top);
        self.0.set_progress(progress).map_err(|_| Interrupt)
    }

    fn get_centroid_layer_statistics(
        &self,
        layer_from_top: usize,
    ) -> Result<Option<LayerStatistics>, Interrupt> {
        Ok(self
            .0
            .progress()
            .and_then(|p| p.centroid_statistics.get(&layer_from_top))
            .copied())
    }

    fn invalidate_centroid_layer_statistics(
        &mut self,
        layer_from_top: usize,
    ) -> Result<(), Interrupt> {
        let progress = self.0.progress();
        if progress.is_none() {
            return Ok(());
        }
        let mut progress = progress.unwrap().clone();
        progress.centroid_statistics.remove(&layer_from_top);
        self.0.set_progress(progress).map_err(|_| Interrupt)
    }
}
