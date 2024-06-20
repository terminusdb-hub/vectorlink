use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use itertools::Either;
use parallel_hnsw::{
    parameters::{BuildParameters, OptimizationParameters, SearchParameters},
    pq::QuantizedHnsw,
    progress::{ProgressMonitor, SimpleProgressMonitor},
    AbstractVector, Hnsw, Serializable, VectorId,
};
use rayon::iter::IndexedParallelIterator;
use serde::{Deserialize, Serialize};

use crate::{
    comparator::{
        Centroid16Comparator, Centroid16Comparator1024, Centroid32Comparator, Centroid4Comparator,
        Centroid8Comparator, Disk1024Comparator, DiskOpenAIComparator, OpenAIComparator,
        Quantized16Comparator, Quantized16Comparator1024, Quantized32Comparator,
        Quantized4Comparator, Quantized8Comparator,
    },
    openai::Model,
    vecmath::{
        Embedding, Embedding1024, CENTROID_16_LENGTH, CENTROID_32_LENGTH, CENTROID_4_LENGTH,
        CENTROID_8_LENGTH, EMBEDDING_LENGTH, EMBEDDING_LENGTH_1024, QUANTIZED_16_EMBEDDING_LENGTH,
        QUANTIZED_16_EMBEDDING_LENGTH_1024, QUANTIZED_32_EMBEDDING_LENGTH,
        QUANTIZED_4_EMBEDDING_LENGTH, QUANTIZED_8_EMBEDDING_LENGTH,
    },
    vectors::VectorStore,
};

pub type OpenAIHnsw = Hnsw<OpenAIComparator>;

#[derive(Serialize, Deserialize)]
pub enum HnswConfigurationType {
    QuantizedOpenAi,
    SmallQuantizedOpenAi,
    SmallQuantizedOpenAi8,
    SmallQuantizedOpenAi4,
    UnquantizedOpenAi,
    Quantized1024,
}

#[derive(Serialize, Deserialize)]
pub struct HnswConfigurationState {
    version: usize,
    #[serde(rename = "type")]
    typ: HnswConfigurationType,
    model: Model,
}

pub enum HnswConfiguration {
    QuantizedOpenAi(
        Model,
        QuantizedHnsw<
            EMBEDDING_LENGTH,
            CENTROID_32_LENGTH,
            QUANTIZED_32_EMBEDDING_LENGTH,
            Centroid32Comparator,
            Quantized32Comparator,
            DiskOpenAIComparator,
        >,
    ),
    SmallQuantizedOpenAi(
        Model,
        QuantizedHnsw<
            EMBEDDING_LENGTH,
            CENTROID_16_LENGTH,
            QUANTIZED_16_EMBEDDING_LENGTH,
            Centroid16Comparator,
            Quantized16Comparator,
            DiskOpenAIComparator,
        >,
    ),
    SmallQuantizedOpenAi8(
        Model,
        QuantizedHnsw<
            EMBEDDING_LENGTH,
            CENTROID_8_LENGTH,
            QUANTIZED_8_EMBEDDING_LENGTH,
            Centroid8Comparator,
            Quantized8Comparator,
            DiskOpenAIComparator,
        >,
    ),
    SmallQuantizedOpenAi4(
        Model,
        QuantizedHnsw<
            EMBEDDING_LENGTH,
            CENTROID_4_LENGTH,
            QUANTIZED_4_EMBEDDING_LENGTH,
            Centroid4Comparator,
            Quantized4Comparator,
            DiskOpenAIComparator,
        >,
    ),
    UnquantizedOpenAi(Model, OpenAIHnsw),
    Quantized1024By16(
        Model,
        QuantizedHnsw<
            EMBEDDING_LENGTH_1024,
            CENTROID_16_LENGTH,
            QUANTIZED_16_EMBEDDING_LENGTH_1024,
            Centroid16Comparator1024,
            Quantized16Comparator1024,
            Disk1024Comparator,
        >,
    ),
}

impl HnswConfiguration {
    fn state(&self) -> HnswConfigurationState {
        let (typ, model) = match self {
            HnswConfiguration::QuantizedOpenAi(model, _) => {
                (HnswConfigurationType::QuantizedOpenAi, model)
            }
            HnswConfiguration::SmallQuantizedOpenAi(model, _) => {
                (HnswConfigurationType::SmallQuantizedOpenAi, model)
            }
            HnswConfiguration::UnquantizedOpenAi(model, _) => {
                (HnswConfigurationType::UnquantizedOpenAi, model)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(model, _) => {
                (HnswConfigurationType::SmallQuantizedOpenAi8, model)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(model, _) => {
                (HnswConfigurationType::SmallQuantizedOpenAi4, model)
            }
            HnswConfiguration::Quantized1024By16(model, _) => {
                (HnswConfigurationType::Quantized1024, model)
            }
        };
        let version = 1;

        HnswConfigurationState {
            version,
            typ,
            model: *model,
        }
    }

    pub fn model(&self) -> Model {
        match self {
            HnswConfiguration::QuantizedOpenAi(m, _) => *m,
            HnswConfiguration::SmallQuantizedOpenAi(m, _) => *m,
            HnswConfiguration::UnquantizedOpenAi(m, _) => *m,
            HnswConfiguration::SmallQuantizedOpenAi8(m, _) => *m,
            HnswConfiguration::SmallQuantizedOpenAi4(m, _) => *m,
            HnswConfiguration::Quantized1024By16(m, _) => *m,
        }
    }

    #[allow(dead_code)]
    pub fn vector_count(&self) -> usize {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => q.vector_count(),
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => q.vector_count(),
            HnswConfiguration::UnquantizedOpenAi(_model, h) => h.vector_count(),
            HnswConfiguration::SmallQuantizedOpenAi8(_model, q) => q.vector_count(),
            HnswConfiguration::SmallQuantizedOpenAi4(_model, q) => q.vector_count(),
            HnswConfiguration::Quantized1024By16(_, q) => q.vector_count(),
        }
    }

    pub fn search(
        &self,
        v: AbstractVector<Embedding>,
        search_parameters: SearchParameters,
    ) -> Vec<(VectorId, f32)> {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => q.search(v, search_parameters),
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => q.search(v, search_parameters),
            HnswConfiguration::UnquantizedOpenAi(_model, h) => h.search(v, search_parameters),
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => q.search(v, search_parameters),
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => q.search(v, search_parameters),
            HnswConfiguration::Quantized1024By16(_, _q) => {
                panic!();
            }
        }
    }

    pub fn search_1024(
        &self,
        v: AbstractVector<Embedding1024>,
        search_parameters: SearchParameters,
    ) -> Vec<(VectorId, f32)> {
        match self {
            HnswConfiguration::Quantized1024By16(_, q) => q.search(v, search_parameters),
            _ => {
                panic!();
            }
        }
    }

    pub fn improve_index(
        &mut self,
        build_parameters: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> f32 {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => {
                q.improve_index(build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => {
                q.improve_index(build_parameters, progress)
            }
            HnswConfiguration::UnquantizedOpenAi(_model, h) => {
                h.improve_index(build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.improve_index(build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.improve_index(build_parameters, progress)
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                q.improve_index(build_parameters, progress)
            }
        }
    }

    pub fn improve_index_at(
        &mut self,
        layer: usize,
        build_parameters: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> (f32, usize) {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => {
                q.improve_index_at(layer, build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => {
                q.improve_index_at(layer, build_parameters, progress)
            }
            HnswConfiguration::UnquantizedOpenAi(_model, h) => {
                h.improve_index_at(layer, build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.improve_index_at(layer, build_parameters, progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.improve_index_at(layer, build_parameters, progress)
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                q.improve_index_at(layer, build_parameters, progress)
            }
        }
    }

    pub fn improve_neighbors(
        &mut self,
        optimization_parameters: OptimizationParameters,
        last_recall: Option<f32>,
    ) -> f32 {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => {
                q.improve_neighbors(optimization_parameters, last_recall)
            }
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => {
                q.improve_neighbors(optimization_parameters, last_recall)
            }
            HnswConfiguration::UnquantizedOpenAi(_model, h) => {
                h.improve_neighbors(optimization_parameters, last_recall)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.improve_neighbors(optimization_parameters, last_recall)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.improve_neighbors(optimization_parameters, last_recall)
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                q.improve_neighbors(optimization_parameters, last_recall)
            }
        }
    }

    pub fn promote_at_layer(
        &mut self,
        layer_from_top: usize,
        build_parameters: BuildParameters,
    ) -> bool {
        let mut progress = SimpleProgressMonitor::default();
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => {
                q.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => {
                q.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
            HnswConfiguration::UnquantizedOpenAi(_model, h) => {
                h.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                q.promote_at_layer(layer_from_top, build_parameters, &mut progress)
            }
        }
    }

    pub fn zero_neighborhood_size(&self) -> usize {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => q.zero_neighborhood_size(),
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => q.zero_neighborhood_size(),
            HnswConfiguration::UnquantizedOpenAi(_model, h) => h.zero_neighborhood_size(),
            HnswConfiguration::SmallQuantizedOpenAi8(_model, q) => q.zero_neighborhood_size(),
            HnswConfiguration::SmallQuantizedOpenAi4(_model, q) => q.zero_neighborhood_size(),
            HnswConfiguration::Quantized1024By16(_model, q) => q.zero_neighborhood_size(),
        }
    }
    pub fn threshold_nn(
        &self,
        threshold: f32,
        search_parameters: SearchParameters,
    ) -> impl IndexedParallelIterator<Item = (VectorId, Vec<(VectorId, f32)>)> + '_ {
        match self {
            HnswConfiguration::QuantizedOpenAi(_model, q) => {
                Either::Left(q.threshold_nn(threshold, search_parameters))
            }
            HnswConfiguration::SmallQuantizedOpenAi(_model, q) => {
                Either::Right(Either::Left(q.threshold_nn(threshold, search_parameters)))
            }
            HnswConfiguration::UnquantizedOpenAi(_model, h) => Either::Right(Either::Right(
                Either::Left(h.threshold_nn(threshold, search_parameters)),
            )),
            HnswConfiguration::SmallQuantizedOpenAi8(_model, q) => Either::Right(Either::Right(
                Either::Right(Either::Left(q.threshold_nn(threshold, search_parameters))),
            )),
            HnswConfiguration::SmallQuantizedOpenAi4(_model, q) => {
                Either::Right(Either::Right(Either::Right(Either::Right(Either::Left(
                    q.threshold_nn(threshold, search_parameters),
                )))))
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                Either::Right(Either::Right(Either::Right(Either::Right(Either::Right(
                    q.threshold_nn(threshold, search_parameters),
                )))))
            }
        }
    }

    pub fn stochastic_recall(&self, optimization_parameters: OptimizationParameters) -> f32 {
        match self {
            HnswConfiguration::QuantizedOpenAi(_, q) => {
                q.stochastic_recall(optimization_parameters)
            }
            HnswConfiguration::SmallQuantizedOpenAi(_, q) => {
                q.stochastic_recall(optimization_parameters)
            }
            HnswConfiguration::UnquantizedOpenAi(_, h) => {
                h.stochastic_recall(optimization_parameters)
            }
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.stochastic_recall(optimization_parameters)
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.stochastic_recall(optimization_parameters)
            }
            HnswConfiguration::Quantized1024By16(_, q) => {
                q.stochastic_recall(optimization_parameters)
            }
        }
    }

    pub fn build_parameters_for_improve_index(&self) -> BuildParameters {
        match self {
            HnswConfiguration::QuantizedOpenAi(_, q) => q.build_parameters_for_improve_index(),
            HnswConfiguration::SmallQuantizedOpenAi(_, q) => q.build_parameters_for_improve_index(),
            HnswConfiguration::SmallQuantizedOpenAi8(_, q) => {
                q.build_parameters_for_improve_index()
            }
            HnswConfiguration::SmallQuantizedOpenAi4(_, q) => {
                q.build_parameters_for_improve_index()
            }
            HnswConfiguration::UnquantizedOpenAi(_, h) => h.build_parameters,
            HnswConfiguration::Quantized1024By16(_, q) => q.build_parameters_for_improve_index(),
        }
    }
}

impl Serializable for HnswConfiguration {
    type Params = Arc<VectorStore>;

    fn serialize<P: AsRef<std::path::Path>>(
        &self,
        path: P,
    ) -> Result<(), parallel_hnsw::SerializationError> {
        match self {
            HnswConfiguration::UnquantizedOpenAi(_, hhnsw) => hhnsw.serialize(&path)?,
            HnswConfiguration::QuantizedOpenAi(_, qnsw) => qnsw.serialize(&path)?,
            HnswConfiguration::SmallQuantizedOpenAi(_, qhnsw) => qhnsw.serialize(&path)?,
            HnswConfiguration::SmallQuantizedOpenAi8(_, qhnsw) => qhnsw.serialize(&path)?,
            HnswConfiguration::SmallQuantizedOpenAi4(_, qhnsw) => qhnsw.serialize(&path)?,
            HnswConfiguration::Quantized1024By16(_, qhnsw) => qhnsw.serialize(&path)?,
        }
        let state_path: PathBuf = path.as_ref().join("state.json");
        let mut state_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(state_path)?;
        serde_json::to_writer(&mut state_file, &self.state())?;
        state_file.sync_data()?;

        Ok(())
    }

    fn deserialize<P: AsRef<std::path::Path>>(
        path: P,
        params: Self::Params,
    ) -> Result<Self, parallel_hnsw::SerializationError> {
        let state_path: PathBuf = path.as_ref().join("state.json");
        let mut state_file = OpenOptions::new()
            .create(false)
            .read(true)
            .open(state_path)?;

        let state: HnswConfigurationState = serde_json::from_reader(&mut state_file)?;

        Ok(match state.typ {
            HnswConfigurationType::QuantizedOpenAi => HnswConfiguration::QuantizedOpenAi(
                state.model,
                QuantizedHnsw::deserialize(path, params)?,
            ),
            HnswConfigurationType::UnquantizedOpenAi => {
                HnswConfiguration::UnquantizedOpenAi(state.model, Hnsw::deserialize(path, params)?)
            }
            HnswConfigurationType::SmallQuantizedOpenAi => HnswConfiguration::SmallQuantizedOpenAi(
                state.model,
                QuantizedHnsw::deserialize(path, params)?,
            ),
            HnswConfigurationType::SmallQuantizedOpenAi8 => {
                HnswConfiguration::SmallQuantizedOpenAi8(
                    state.model,
                    QuantizedHnsw::deserialize(path, params)?,
                )
            }
            HnswConfigurationType::SmallQuantizedOpenAi4 => {
                HnswConfiguration::SmallQuantizedOpenAi4(
                    state.model,
                    QuantizedHnsw::deserialize(path, params)?,
                )
            }
            HnswConfigurationType::Quantized1024 => HnswConfiguration::Quantized1024By16(
                state.model,
                QuantizedHnsw::deserialize(path, params)?,
            ),
        })
    }
}
