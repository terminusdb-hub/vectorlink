use rayon::prelude::*;

use crate::{
    parameters::SearchParameters,
    priority_queue::PriorityQueue,
    types::{AbstractVector, NodeId, OrderedFloat, VectorId},
    Comparator, Layer, NodeDistances,
};
pub fn entry_vector<C: Comparator, L: AsRef<Layer<C>>>(layers: &[L]) -> VectorId {
    layers[0].as_ref().nodes[0]
}

pub fn compare_all<C: Comparator>(
    comparator: C,
    v: VectorId,
    vs: &[VectorId],
) -> Vec<(VectorId, f32)> {
    let mut res: Vec<_> = vs
        .iter()
        .filter(|w| **w != v)
        .map(|w| {
            (
                *w,
                comparator.compare_vec(AbstractVector::Stored(v), AbstractVector::Stored(*w)),
            )
        })
        .collect();
    res.sort_by_key(|(v, d)| (OrderedFloat(*d), *v));
    res
}

pub fn generate_initial_partitions<C: Comparator, L: AsRef<Layer<C>> + Sync>(
    vs: &[VectorId],
    comparator: &C,
    initial_vector_search: SearchParameters,
    layers: &[L],
    node_offset: usize,
) -> Vec<(NodeId, VectorId, NodeDistances)> {
    let mut initial_partitions: Vec<(NodeId, VectorId, NodeDistances)> =
        Vec::with_capacity(vs.len());
    vs.par_iter()
        .enumerate()
        .map(|(node_id, vector_id)| (NodeId(node_id + node_offset), vector_id))
        .map(|(node_id, vector_id)| {
            let comparator = comparator.clone();
            let initial_vector_distances = if layers.is_empty() {
                //eprintln!("empty layers");
                compare_all(comparator, *vector_id, vs)
            } else {
                //eprintln!("not empty layers");
                initial_vector_distances(*vector_id, initial_vector_search, layers)
            };
            //eprintln!("ivd: {initial_vector_distances:?}");
            let initial_node_distances: Vec<_> = initial_vector_distances
                .into_iter()
                .map(|(inner_vector_id, distance)| {
                    (
                        NodeId(vs.binary_search(&inner_vector_id).unwrap()),
                        distance,
                    )
                })
                .collect();
            (node_id, *vector_id, initial_node_distances)
        })
        .collect_into_vec(&mut initial_partitions);

    initial_partitions.par_sort_unstable_by_key(|(_node_id, _vector_id, distances)| {
        distances.first().map(|(_, d)| OrderedFloat(*d))
    });
    initial_partitions
}

pub fn initial_vector_distances<C: Comparator, L: AsRef<Layer<C>>>(
    v: VectorId,
    sp: SearchParameters,
    layers: &[L],
) -> Vec<(VectorId, f32)> {
    search_layers(AbstractVector::Stored(v), sp, layers, None)
        .into_iter()
        .filter(|(w, _)| v != *w)
        .collect::<Vec<_>>()
}

pub fn search_layers<C: Comparator, L: AsRef<Layer<C>>>(
    v: AbstractVector<C::T>,
    sp: SearchParameters,
    layers: &[L],
    exclude: Option<VectorId>,
) -> Vec<(VectorId, f32)> {
    search_layers_instrumented(v, sp, layers, exclude).0
}

pub fn search_layers_instrumented<C: Comparator, L: AsRef<Layer<C>>>(
    v: AbstractVector<C::T>,
    sp: SearchParameters,
    layers: &[L],
    exclude: Option<VectorId>,
) -> (Vec<(VectorId, f32)>, usize) {
    //let upper_layer_candidate_count = 2;
    let upper_layer_candidate_count = sp.upper_layer_candidate_count;
    let entry_vector = entry_vector(layers);
    let distance_from_entry = layers
        .first()
        .map(|l| {
            l.as_ref()
                .comparator
                .compare_vec(v.clone(), AbstractVector::Stored(entry_vector))
        })
        .unwrap_or(0.0);
    let mut candidates = PriorityQueue::new(sp.number_of_candidates);
    candidates.insert(entry_vector, distance_from_entry);
    let mut last_index_distance = usize::MAX;
    for i in 0..layers.len() {
        candidates
            .iter()
            .fold(f32::MIN, |last, (nodeid, distance)| {
                if distance < last {
                    panic!("oh yikes {nodeid:?} {distance}");
                }
                distance
            });
        let candidate_count = if layers.len() == 1 || i == layers.len() - 1 {
            sp.number_of_candidates
        } else {
            upper_layer_candidate_count
        };
        let layer = &layers[i];
        let (closest, index_distance) = layer.as_ref().closest_vectors(
            v.clone(),
            &candidates,
            candidate_count,
            sp.probe_depth,
            |v| Some(v) != exclude,
        );
        last_index_distance = index_distance;
        candidates.merge_pairs(&closest);
    }

    (candidates.iter().collect(), last_index_distance)
}

pub fn assert_layer_invariants<C: Comparator, L: AsRef<Layer<C>>>(layers: &[L]) {
    if layers.len() <= 1 {
        return;
    }
    for i in 0..layers.len() - 1 {
        let current_layer = layers[i].as_ref();
        let next_layer = layers[i + 1].as_ref();
        let mut last_node: Option<VectorId> = None;
        for node in current_layer.nodes.iter() {
            if let Some(last_node) = last_node {
                if *node <= last_node {
                    panic!("Layer did not meet invariants, nodes are not monotonic {node:?} > {last_node:?}, layer_from_top {}", i);
                }
            }
            last_node = Some(*node);
            if next_layer.nodes.binary_search(node).is_err() {
                //eprintln!("{:?}", layers[i].as_ref().nodes);
                eprintln!("{:?}", layers[i].as_ref().neighbors);

                //eprintln!("{:?}", layers[i + 1].as_ref().nodes);
                eprintln!("{:?}", layers[i + 1].as_ref().neighbors);

                panic!(
                    "Layer did not meet invariants, missing: {node:?} in layer_from_top {}",
                    i + 1
                );
            }
        }
    }
}

pub fn match_within_epsilon(vector: VectorId, matches: Vec<(VectorId, f32)>) -> bool {
    let mut vector_is_in_matches = false;
    let epsilon = 1e-5;
    for m in matches.into_iter() {
        let d = m.1;
        if d.abs() < epsilon {
            if m.0 == vector {
                vector_is_in_matches = true;
            }
        } else {
            break;
        }
    }
    vector_is_in_matches
}
