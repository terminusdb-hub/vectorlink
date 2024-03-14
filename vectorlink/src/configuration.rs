use std::{fs::OpenOptions, path::PathBuf, sync::Arc};

use parallel_hnsw::{pq::QuantizedHnsw, Hnsw, Serializable};
use serde::{Deserialize, Serialize};

use crate::{
    comparator::{Centroid32Comparator, OpenAIComparator, QuantizedComparator},
    openai::Model,
    vecmath::{CENTROID_32_LENGTH, EMBEDDING_LENGTH, QUANTIZED_EMBEDDING_LENGTH},
    vectors::VectorStore,
};

pub type OpenAIHnsw = Hnsw<OpenAIComparator>;

#[derive(Serialize, Deserialize)]
pub enum HnswConfigurationType {
    QuantizedOpenAi,
    UnquantizedOpenAi,
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
            QUANTIZED_EMBEDDING_LENGTH,
            Centroid32Comparator,
            QuantizedComparator,
            OpenAIComparator,
        >,
    ),
    UnquantizedOpenAi(Model, OpenAIHnsw),
}

impl HnswConfiguration {
    fn state(&self) -> HnswConfigurationState {
        let (typ, model) = match self {
            HnswConfiguration::QuantizedOpenAi(model, _) => {
                (HnswConfigurationType::QuantizedOpenAi, model)
            }
            HnswConfiguration::UnquantizedOpenAi(model, _) => {
                (HnswConfigurationType::UnquantizedOpenAi, model)
            }
        };
        let version = 1;

        HnswConfigurationState {
            version,
            typ,
            model: *model,
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
            HnswConfiguration::QuantizedOpenAi(_, hnsw) => {
                hnsw.serialize(&path)?;
            }
            HnswConfiguration::UnquantizedOpenAi(_, qhnsw) => {
                qhnsw.serialize(&path)?;
            }
        }
        let state_path: PathBuf = path.as_ref().join("state.json");
        let mut state_file = OpenOptions::new()
            .create(true)
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
        let mut state_path: PathBuf = path.as_ref().join("state.json");
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
        })
    }
}