use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parallel_hnsw::parameters::OptimizationParameters;
use parallel_hnsw::Serializable;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::task::block_in_place;
use vectorlink::openai::Model;
use vectorlink::vectors::VectorStore;
use vectorlink::{batch::index_domain, configuration::HnswConfiguration};
use vectorlink_task::task::{SyncTaskLiveness, TaskHandler, TaskLiveness};

use parallel_hnsw::progress::{Interrupt, LayerStatistics, ProgressMonitor};

#[derive(Clone, Serialize, Deserialize)]
pub struct IndexingRequest {
    domain: String,
    commit: String,
    directory: String,
    model: Model,
    quantized: bool,
    operation: IndexOperation,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum IndexOperation {
    BuildIndex,
    ImproveIndex {
        optimization_parameters: Option<OptimizationParameters>,
    },
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
    type Init = IndexingRequest;

    // TODO: actual progress should not be an arbitrary json object but a meaningful serializable state object.
    type Progress = IndexProgress;

    type Complete = ();

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
        let request: IndexingRequest = live.init().unwrap().unwrap();
        let IndexingRequest {
            model,
            directory,
            domain,
            commit,
            quantized,
            operation,
        } = request;
        let _state = live.progress().unwrap();
        let live = live.into_sync().unwrap();

        let mut monitor = TaskMonitor(live);
        block_in_place(|| match operation {
            IndexOperation::BuildIndex => {
                index_domain(
                    key,
                    model,
                    directory,
                    &domain,
                    &commit,
                    12345,
                    quantized,
                    &mut monitor,
                )
                .unwrap();
            }
            IndexOperation::ImproveIndex {
                optimization_parameters,
            } => {
                let store = VectorStore::new(&directory, 12345);
                let mut hnsw: HnswConfiguration =
                    HnswConfiguration::deserialize(&directory, Arc::new(store)).unwrap();
                let mut build_parameters = hnsw.build_parameters_for_improve_index();
                if let Some(optimization_parameters) = optimization_parameters {
                    build_parameters.optimization = optimization_parameters;
                }
                hnsw.improve_index(build_parameters, &mut monitor);
            }
        });

        Ok(())
    }
}

struct TaskMonitor(SyncTaskLiveness<IndexingRequest, IndexProgress>);

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

#[cfg(test)]
mod tests {
    use crate::handler::IndexOperation;

    #[test]
    fn serialization_test() {
        let io = IndexOperation::ImproveIndex {
            optimization_parameters: None,
        };
        let s1 = serde_json::to_string(&io).unwrap();

        let bi = IndexOperation::BuildIndex;
        let s2 = serde_json::to_string(&bi).unwrap();

        panic!("{}", s2);
    }
}
