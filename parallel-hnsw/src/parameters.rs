use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct SearchParameters {
    pub number_of_candidates: usize,
    pub upper_layer_candidate_count: usize,
    pub probe_depth: usize,
    pub circulant_parameter_count: usize,
    pub random_link_count: usize,
}

impl Default for SearchParameters {
    fn default() -> Self {
        Self {
            number_of_candidates: 300,
            upper_layer_candidate_count: 300,
            probe_depth: 2,
            circulant_parameter_count: 6,
            random_link_count: 0,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct OptimizationParameters {
    pub promotion_threshold: f32,
    pub neighborhood_threshold: f32,
    pub recall_confidence: f32,
    pub promotion_proportion: f32,
    pub search: SearchParameters,
}

impl Default for OptimizationParameters {
    fn default() -> Self {
        Self {
            promotion_threshold: 0.01,
            neighborhood_threshold: 0.01,
            recall_confidence: 0.99,
            promotion_proportion: 0.0,
            search: SearchParameters::default(),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct BuildParameters {
    pub order: usize,
    pub zero_layer_neighborhood_size: usize,
    pub neighborhood_size: usize,
    pub optimization: OptimizationParameters,
    pub initial_partition_search: SearchParameters,
    pub max_recall_threshold: Option<f32>,
}

impl Default for BuildParameters {
    fn default() -> Self {
        Self {
            order: 12,
            zero_layer_neighborhood_size: 48,
            neighborhood_size: 24,
            optimization: Default::default(),
            initial_partition_search: SearchParameters {
                number_of_candidates: 6,
                upper_layer_candidate_count: 6,
                probe_depth: 2,
                circulant_parameter_count: 0,
                random_link_count: 0,
            },
            max_recall_threshold: None,
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct PqBuildParameters {
    pub centroids: BuildParameters,
    pub hnsw: BuildParameters,
    pub quantized_search: SearchParameters,
}

impl Default for PqBuildParameters {
    fn default() -> Self {
        Self {
            centroids: Default::default(),
            hnsw: Default::default(),
            quantized_search: SearchParameters {
                number_of_candidates: 10,
                upper_layer_candidate_count: 6,
                probe_depth: 2,
                circulant_parameter_count: 0,
                random_link_count: 0,
            },
        }
    }
}
