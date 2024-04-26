pub mod bigvec;
pub mod parameters;
pub mod pq;
mod priority_queue;
pub mod progress;
mod search;
pub mod serialize;
mod types;

use parameters::{BuildParameters, OptimizationParameters, SearchParameters};
use progress::ProgressMonitor;
pub use serialize::SerializationError;
pub use types::*;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Deref,
    path::Path,
    slice::Iter,
    sync::{
        atomic::{self, AtomicUsize},
        RwLock,
    },
};

use itertools::Itertools;
use rand::{rngs::StdRng, seq::SliceRandom, thread_rng, Rng, SeedableRng};
use rand_distr::{Distribution, Exp};
use rayon::prelude::*;
use std::fmt::Debug;

use crate::search::assert_layer_invariants;
use crate::{priority_queue::PriorityQueue, search::match_within_epsilon};

pub enum WrappedBorrowable<'a, T: ?Sized, Borrowable: Deref<Target = T> + 'a> {
    Left(Borrowable),
    Right(&'a T),
}

impl<'a, T: ?Sized, Borrowable: Deref<Target = T> + 'a> Deref
    for WrappedBorrowable<'a, T, Borrowable>
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            WrappedBorrowable::Left(b) => b,
            WrappedBorrowable::Right(b) => b,
        }
    }
}

pub trait Comparator: Sync + Clone {
    type T;
    type Borrowable<'a>: Deref<Target = Self::T>
    where
        Self: 'a;
    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_>;
    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32;
    fn lookup_abstract<'a: 'b, 'b>(
        &'a self,
        v: AbstractVector<'b, Self::T>,
    ) -> WrappedBorrowable<'b, Self::T, Self::Borrowable<'b>> {
        match v {
            AbstractVector::Stored(i) => WrappedBorrowable::Left(self.lookup(i)),
            AbstractVector::Unstored(v) => WrappedBorrowable::Right(v),
        }
    }
    fn compare_vec(&self, v1: AbstractVector<Self::T>, v2: AbstractVector<Self::T>) -> f32 {
        let v1 = self.lookup_abstract(v1);
        let v2 = self.lookup_abstract(v2);
        self.compare_raw(&*v1, &*v2)
    }
}

pub trait Serializable: Sized {
    type Params;
    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError>;
    fn deserialize<P: AsRef<Path>>(
        path: P,
        params: Self::Params,
    ) -> Result<Self, SerializationError>;
}

#[derive(PartialEq, PartialOrd, Debug)]
pub struct Layer<C> {
    pub comparator: C,
    pub neighborhood_size: usize,
    pub nodes: Vec<VectorId>,
    pub neighbors: Vec<NodeId>,
}

impl<C: Comparator> Clone for Layer<C> {
    fn clone(&self) -> Self {
        Self {
            comparator: self.comparator.clone(),
            neighborhood_size: self.neighborhood_size,
            nodes: self.nodes.clone(),
            neighbors: self.neighbors.clone(),
        }
    }
}

unsafe impl<C: Comparator> Sync for Layer<C> {}

impl<C: Comparator> AsRef<Layer<C>> for Layer<C> {
    fn as_ref(&self) -> &Layer<C> {
        self
    }
}

type NodeDistances = Vec<(NodeId, f32)>;

fn get_final_neighbor_idx(neighborhood_size: usize, neighbors: &[NodeId], n: NodeId) -> usize {
    let final_idx = neighborhood_size * (n.0 + 1);
    let mut current_idx = final_idx;
    for offset in 1..neighborhood_size + 1 {
        if neighbors[final_idx - offset].0 == !0 {
            current_idx -= 1;
        } else {
            break;
        }
    }
    current_idx
}

impl<C> Layer<C> {
    #[allow(unused)]
    pub fn get_node(&self, v: VectorId) -> Option<NodeId> {
        self.nodes.binary_search(&v).ok().map(NodeId)
    }

    pub fn get_vector(&self, n: NodeId) -> VectorId {
        if n.0 > self.nodes.len() {
            eprintln!("n.0: {} for node length {}", n.0, self.nodes.len());
        }
        self.nodes[n.0]
    }

    pub fn get_final_neighbor_idx(&self, n: NodeId) -> usize {
        get_final_neighbor_idx(self.neighborhood_size, &self.neighbors, n)
    }

    pub fn get_neighbors(&self, n: NodeId) -> &[NodeId] {
        let first_idx = self.neighborhood_size * n.0;
        let final_idx = self.get_final_neighbor_idx(n);
        &self.neighbors[first_idx..final_idx]
    }

    pub fn get_neighborhood_mut(&mut self, n: NodeId) -> &mut [NodeId] {
        let first_idx = self.neighborhood_size * n.0;
        let final_idx = self.neighborhood_size * (n.0 + 1);
        &mut self.neighbors[first_idx..final_idx]
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn routing_nodes(&self, nodeid: NodeId, sp: SearchParameters) -> Vec<NodeId> {
        if sp.grid_network_dimension == 0 {
            return Vec::new();
        }
        let increment = self.node_count() / sp.grid_network_dimension;
        let mut routing_nodes = Vec::with_capacity(sp.grid_network_dimension);
        for i in 1..sp.grid_network_dimension + 1 {
            let new_node_id = (i * increment + nodeid.0) % self.node_count();
            routing_nodes.push(NodeId(new_node_id))
        }
        routing_nodes
    }
}

impl<C: Comparator> Layer<C> {
    pub fn nearest_neighbors(
        &self,
        n: NodeId,
        number_of_nodes: usize,
        search_parameters: SearchParameters,
    ) -> Vec<(NodeId, f32)> {
        let v = self.get_vector(n);
        let mut candidates = PriorityQueue::new(number_of_nodes);
        candidates.insert(n, f32::MAX);
        self.closest_nodes(
            AbstractVector::Stored(v),
            &mut candidates,
            search_parameters.number_of_candidates,
            search_parameters,
        );
        candidates.iter().collect()
    }

    pub fn closest_nodes(
        &self,
        v: AbstractVector<C::T>,
        candidates: &mut PriorityQueue<NodeId>,
        mut probe_depth: usize,
        sp: SearchParameters,
    ) -> usize {
        assert!(!candidates.is_empty());
        let mut visit_queue: Vec<(NodeId, f32, NodeDistance)> = candidates
            .iter()
            .map(|(n, f)| (n, f, NodeDistance::ZERO))
            .collect();
        visit_queue.reverse();
        let mut visited: HashSet<NodeId> = candidates.iter().map(|(n, _)| n).collect();
        //eprintln!("------------------------------------");
        //eprintln!("Initial visit queue: {visit_queue:?}");
        let mut highest_improvement = 0;
        while let Some((next, _, node_distance)) = visit_queue.pop() {
            //eprintln!("...");
            //eprintln!("working with next: {next:?}");
            //visited.insert(next);
            let neighbors = self.get_neighbors(next);
            let mut neighbor_distances: Vec<_> = neighbors
                .iter() // Remove reviously visited nodes
                .chain(self.routing_nodes(next, sp).iter())
                .filter(|n| !visited.contains(*n))
                .map(|n| {
                    let distance = self
                        .comparator
                        .compare_vec(v.clone(), AbstractVector::Stored(self.get_vector(*n)));
                    (*n, distance)
                })
                .collect();
            neighbor_distances.sort_by_key(|(n, distance)| (OrderedFloat(*distance), *n));

            //eprintln!("calculated neighbor_distances@{next:?}: {neighbor_distances:?}");
            visited.extend(neighbor_distances.iter().map(|(n, _)| n));

            visit_queue.extend(neighbor_distances.iter().enumerate().map(|(ix, (n, d))| {
                (
                    *n,
                    *d,
                    NodeDistance {
                        hops: node_distance.hops + 1,
                        index_sum: node_distance.index_sum + ix + 1,
                    },
                )
            }));
            //eprintln!("before");
            //dbg!(&candidates.data);
            //dbg!(&candidates.priorities);

            let current_best = candidates.first();
            let did_something = candidates.merge_pairs(&neighbor_distances);
            if current_best != candidates.first() {
                // an improvement was made
                highest_improvement = node_distance.index_sum;
            }
            //eprintln!("after");

            if !did_something {
                probe_depth -= 1;
                if probe_depth == 0 {
                    break;
                }
            }
            //dbg!(&candidates.data);
            //dbg!(&candidates.priorities);

            // Sort in reverse order
            visit_queue
                .sort_by_key(|(n, distance, _)| (OrderedFloat(-*distance), (usize::MAX - n.0)))
        }

        highest_improvement
    }

    pub fn closest_vectors<F: Fn(VectorId) -> bool>(
        &self,
        v: AbstractVector<C::T>,
        candidates: &PriorityQueue<VectorId>,
        candidate_count: usize,
        search_parameters: SearchParameters,
        include: F,
    ) -> (Vec<(VectorId, f32)>, usize) {
        let pairs: Vec<(NodeId, f32)> = candidates
            .iter()
            // We should only be proceeding downwards!
            .map(|(v, d)| (self.get_node(v).unwrap(), d))
            .collect();
        //eprintln!("pairs: {pairs:?}");
        let mut queue = PriorityQueue::new(candidates.capacity());
        //let mut queue = PriorityQueue::new(candidate_count);
        queue.merge_pairs(&pairs);
        let index_distance = self.closest_nodes(
            v,
            &mut queue,
            search_parameters.number_of_candidates,
            search_parameters,
        );
        (
            queue
                .iter()
                .map(|(node_id, distance)| (self.get_vector(node_id), distance))
                .filter(|(v, _)| include(*v))
                .take(candidate_count)
                .collect(),
            index_distance,
        )
    }

    pub fn group_nodes_by_vectors(
        &self,
        vectors: &[VectorId],
    ) -> HashMap<VectorId, Vec<(NodeId, f32)>> {
        // partition nodes by distance to supers
        let mut distances_to_supers: Vec<_> = self
            .nodes
            .par_iter()
            .enumerate()
            .map(|(ix, v)| {
                let node = NodeId(ix);
                let super_distances = (0..vectors.len()).map(|ix2| {
                    let super_vec = vectors[ix2];
                    (
                        super_vec,
                        self.comparator.compare_vec(
                            AbstractVector::Stored(*v),
                            AbstractVector::Stored(super_vec),
                        ),
                    )
                });
                let best_super = super_distances
                    .min_by_key(|(_, d)| OrderedFloat(*d))
                    .unwrap();

                (best_super.0, node, best_super.1)
            })
            .collect();

        distances_to_supers.sort_by_key(|(s, _, _)| s.0);
        let partitions: HashMap<_, _> = distances_to_supers
            .into_iter()
            .group_by(|(s, _, _)| *s)
            .into_iter()
            .map(|(super_vec, group)| {
                let nodes: Vec<_> = group.map(|(_, n, d)| (n, d)).collect();

                (super_vec, nodes)
            })
            .collect();

        partitions
    }

    pub fn multi_node_distances<const N: usize>(
        &self,
        supers: &[VectorId],
    ) -> Vec<[(VectorId, NodeDistance); N]> {
        let mut visit_queue = Vec::with_capacity(supers.len());
        visit_queue.extend(supers.iter().map(|s| (0, self.get_node(*s).unwrap(), *s)));
        let mut result = vec![[(VectorId::MAX, NodeDistance::MAX); N]; self.node_count()];
        for (_, node, super_vec) in visit_queue.iter() {
            let result_entry = &mut result[node.0];
            result_entry[0] = (
                *super_vec,
                NodeDistance {
                    hops: 0,
                    index_sum: 0,
                },
            );
        }

        let mut generation = 1;
        while !visit_queue.is_empty() {
            let mut next_visit_queue = Vec::new();
            // do stuff
            for (_, node, super_vec) in visit_queue {
                let current_distance = result[node.0]
                    .iter()
                    .find(|(v, _)| *v == super_vec)
                    .unwrap()
                    .1;
                for (ix, neighbor) in self
                    .get_neighbors(node)
                    .iter()
                    .filter(|n| !n.is_empty())
                    .enumerate()
                {
                    if !result[neighbor.0].iter().any(|(v, _)| *v == super_vec) {
                        if let Some(insert_pos) = result[neighbor.0]
                            .iter_mut()
                            .find(|(v, _)| *v == VectorId::MAX)
                        {
                            insert_pos.0 = super_vec;
                            insert_pos.1 = NodeDistance {
                                hops: generation,
                                index_sum: current_distance.index_sum + ix + 1,
                            };

                            next_visit_queue.push((insert_pos.1.index_sum, *neighbor, super_vec));
                        }
                    }
                }

                generation += 1;
            }

            next_visit_queue.sort_by_key(|(distance, _, _)| *distance);

            visit_queue = next_visit_queue;
        }
        eprintln!("generation {generation}");

        result
    }

    pub fn node_distances_from_closest_super(&self, supers: &[VectorId]) -> Vec<NodeDistance> {
        let super_groups = self.group_nodes_by_vectors(supers);
        assert_eq!(
            super_groups.values().map(|g| g.len()).sum::<usize>(),
            self.node_count()
        );
        let distances = self.multi_node_distances::<5>(supers);
        let borrowed_distances = &distances;

        let mut distances: Vec<_> = super_groups
            .into_iter()
            .flat_map(|(super_vec, group)| {
                group.into_iter().map(move |(node, _)| {
                    if let Some((_, d)) = borrowed_distances[node.0]
                        .iter()
                        .find(|(v, _)| *v == super_vec)
                    {
                        (node, *d)
                    } else {
                        (node, NodeDistance::MAX)
                    }
                })
            })
            .collect();
        distances.sort_by_key(|(n, _)| *n);

        distances.into_iter().map(|(_, d)| d).collect()
    }

    pub fn nodes_not_connected_to_super(&self, supers: &[VectorId]) -> Vec<NodeId> {
        let distances = self.node_distances_from_closest_super(supers);
        distances
            .into_iter()
            .enumerate()
            .filter(|(_, d)| d.hops == usize::MAX)
            .map(|(n, _)| NodeId(n))
            .collect()
    }

    /// Find the distance of each node to any supernode
    pub fn node_distances(&self, supers: &[VectorId]) -> Vec<NodeDistance> {
        let mut visit_queue = Vec::with_capacity(supers.len());
        visit_queue.extend(supers.iter().map(|s| self.get_node(*s).unwrap()));

        let mut result: Vec<AtomicNodeDistance> = Vec::with_capacity(self.node_count());
        for _ in 0..self.node_count() {
            result.push(AtomicNodeDistance::new());
        }
        for n in visit_queue.iter() {
            let AtomicNodeDistance { index_sum, .. } = &result[n.0];
            index_sum.store(0, atomic::Ordering::Relaxed);
        }

        let mut generation = 0;
        loop {
            visit_queue = visit_queue
                .into_iter()
                .flat_map(|node| {
                    let AtomicNodeDistance {
                        hops,
                        index_sum: distance,
                    } = &result[node.0];
                    let swap_result = hops.compare_exchange(
                        usize::MAX,
                        generation,
                        atomic::Ordering::Relaxed,
                        atomic::Ordering::Relaxed,
                    );
                    let value_after_swap = match swap_result.as_ref() {
                        Ok(_) => generation,
                        Err(v) => *v,
                    };
                    if value_after_swap == generation {
                        let neighbors = self.get_neighbors(node);
                        for (ix, neighbor) in neighbors.iter().enumerate() {
                            let total_distance = distance.load(atomic::Ordering::Relaxed) + ix + 1;
                            let AtomicNodeDistance { index_sum, .. } = &result[neighbor.0];
                            index_sum
                                .fetch_update(
                                    atomic::Ordering::Relaxed,
                                    atomic::Ordering::Relaxed,
                                    |current| Some(usize::min(current, total_distance)),
                                )
                                .unwrap();
                        }

                        if swap_result.is_ok() {
                            itertools::Either::Left(neighbors.iter().cloned())
                        } else {
                            itertools::Either::Right(std::iter::empty())
                        }
                    } else {
                        itertools::Either::Right(std::iter::empty())
                    }
                })
                .collect();

            if visit_queue.is_empty() {
                break;
            }
            generation += 1;
        }

        result.into_iter().map(|r| r.finalize()).collect()
    }

    pub fn reachables_from(&self, node: NodeId, check: &[NodeId]) -> Vec<(NodeId, usize)> {
        let mut set: HashSet<NodeId> = HashSet::with_capacity(check.len());
        set.extend(check);
        let mut result = vec![(node, 0)];
        let mut visit_queue = vec![(node, 0)];
        while let Some((node, distance)) = visit_queue.pop() {
            let neighbors = self.get_neighbors(node);
            for (ix, n) in neighbors.iter().enumerate() {
                if set.remove(n) {
                    let new_distance = distance + ix + 1;
                    visit_queue.push((*n, new_distance));
                    result.push((*n, new_distance));
                }
            }
        }

        result
    }

    pub fn discover_nodes_to_promote(&self, supers: &[VectorId]) -> Vec<NodeId> {
        let bottom_distances: Vec<NodeDistance> = self.node_distances(supers);

        let mut bottom_distances: Vec<(NodeId, usize, usize)> = bottom_distances
            .into_iter()
            .enumerate()
            .map(|(ix, d)| (NodeId(ix), d.index_sum, d.hops))
            .collect();
        bottom_distances.sort_by_key(|(n, d, h)| (usize::MAX - d, usize::MAX - h, *n));

        let to_promote: Vec<_> = bottom_distances
            .iter()
            .take_while(|(_, _, d)| *d == usize::MAX)
            .map(|(n, _, _)| *n)
            .collect();

        // TODO! Use buget to increase this with the tail from bottom_distances
        let unreachable = bottom_distances
            .iter()
            .take_while(|(_, _, d)| *d == !0)
            .count();
        let less_than_reachable = to_promote.len() - unreachable;
        let total = to_promote.len();

        eprintln!("promoting {total} vectors ({unreachable} fully unreachable, and {less_than_reachable} less-than-reachables)");
        to_promote
    }

    pub fn reverse_get_neighbors(&self, node: NodeId) -> Vec<NodeId> {
        let mut result = Vec::new();
        for n in 0..self.node_count() {
            if self.get_neighbors(NodeId(n)).contains(&node) {
                result.push(NodeId(n));
            }
        }

        result
    }
}

struct AtomicNodeDistance {
    hops: AtomicUsize,
    index_sum: AtomicUsize,
}

impl AtomicNodeDistance {
    fn new() -> Self {
        Self {
            hops: AtomicUsize::new(usize::MAX),
            index_sum: AtomicUsize::new(usize::MAX),
        }
    }

    fn finalize(self) -> NodeDistance {
        unsafe { std::mem::transmute(self) }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Hash, Default)]
pub struct NodeDistance {
    pub hops: usize,
    pub index_sum: usize,
}

impl NodeDistance {
    const ZERO: Self = Self {
        hops: 0,
        index_sum: 0,
    };
    const MAX: Self = Self {
        hops: usize::MAX,
        index_sum: usize::MAX,
    };
}

#[derive(Debug)]
pub struct Hnsw<C> {
    pub layers: Vec<Layer<C>>,
    pub build_parameters: BuildParameters,
}

impl<C> Hnsw<C> {
    pub fn vector_count(&self) -> usize {
        self.get_layer(0).map(|l| l.node_count()).unwrap_or(0)
    }

    pub fn neighborhood_size(&self) -> usize {
        self.build_parameters.neighborhood_size
    }

    pub fn zero_neighborhood_size(&self) -> usize {
        self.build_parameters.zero_layer_neighborhood_size
    }

    pub fn get_layer(&self, i: usize) -> Option<&Layer<C>> {
        self.get_layer_from_top(self.layers.len() - i - 1)
    }

    pub fn get_layer_from_top_mut(&mut self, i: usize) -> Option<&mut Layer<C>> {
        if i < self.layer_count() {
            Some(&mut self.layers[i])
        } else {
            // eprintln!("No layer");
            None
        }
    }

    pub fn get_layer_from_top(&self, i: usize) -> Option<&Layer<C>> {
        if i < self.layer_count() {
            Some(&self.layers[i])
        } else {
            // eprintln!("No layer");
            None
        }
    }

    #[allow(unused)]
    fn layer_from_top_to_layer(&self, layer: usize) -> usize {
        self.layer_count() - layer - 1
    }

    pub fn get_layer_above(&self, i: usize) -> Option<&Layer<C>> {
        if i == 0 {
            None
        } else {
            self.get_layer_from_top(i - 1)
        }
    }

    pub fn entry_vector(&self) -> VectorId {
        // Other choices are possible
        self.layers[0].nodes[0]
    }

    pub fn layer_count(&self) -> usize {
        self.layers.len()
    }

    pub fn comparator(&self) -> &C {
        &self.layers[0].comparator
    }
}

impl<C: Comparator + 'static> Hnsw<C> {
    pub fn search_upto(
        &self,
        v: AbstractVector<C::T>,
        sp: SearchParameters,
        upto_layer_from_top: usize,
    ) -> Vec<(VectorId, f32)> {
        search::search_layers(v, sp, &self.layers[..upto_layer_from_top], None)
    }

    pub fn search(&self, v: AbstractVector<C::T>, sp: SearchParameters) -> Vec<(VectorId, f32)> {
        search::search_layers(v, sp, &self.layers, None)
    }

    pub fn search_instrumented(
        &self,
        v: AbstractVector<C::T>,
        sp: SearchParameters,
    ) -> (Vec<(VectorId, f32)>, usize) {
        search::search_layers_instrumented(v, sp, &self.layers, None)
    }

    pub fn generate_layer(
        &self,
        comparator: C,
        mut vs: Vec<VectorId>,
        neighborhood_size: usize,
        initial_partition_search: SearchParameters,
        new_top: bool,
    ) -> Layer<C> {
        assert!(!vs.is_empty(), "tried to construct an empty layer");
        // sort first, so that caller can send in unsorted
        vs.sort();
        // Parameter for the number of neighbours to look at from the proceeding layer.
        eprintln!("Constructing neighorhood data structure");
        // This is the final neighbors array, we are going to slice it up here
        // and seed it with some prospects.
        let total_size = vs.len() * neighborhood_size;
        let mut neighbors = vec![NodeId(!0); total_size];
        // This is an auxilliary to keep track of the neighbors quality in a neighborhood
        let mut neighbor_distances = vec![f32::MAX; total_size];

        // This was needed to reduce memory pressure
        // we should *really* consider succinct data structures
        eprintln!("Finding partition groups");
        // 1. Calculate our node id, and find our neighborhood in the above layer
        let layers: &[Layer<_>] = if new_top { &[] } else { &self.layers };
        let initial_partitions = search::generate_initial_partitions(
            &vs,
            &comparator,
            initial_partition_search,
            layers,
            0,
        );

        eprintln!("Generating partition groups");
        // 2. Partition the layer in terms of the closeness to the
        // best node in the layer above
        let partition_groups = initial_partitions
            .into_iter()
            .into_group_map_by(|(_, _, distances)| distances.first().map(|(id, _)| *id));

        // 3. Calculate our neighbourhoods by comparing distances in our partition
        let borrowed_comparator = &comparator;

        eprintln!("Scanning partition groups");
        partition_groups.par_iter().for_each(|(_sup, partition)| {
            partition
                .par_iter()
                .for_each(|(node_id, vector_id, distances)| {
                    let mut distances = distances.clone();
                    let super_nodes: Vec<_> =
                        distances.iter().map(|(node, _)| node).cloned().collect();

                    // some random, some for neighborhood
                    // TODO - also some random extra nodes on the same layer
                    let mut prng = StdRng::seed_from_u64(
                        self.layer_count() as u64 + vector_id.0 as u64 + vs.len() as u64,
                    );

                    let mut partitions: Vec<_> = super_nodes
                        .into_iter()
                        .filter_map(|n| partition_groups.get(&Some(n)))
                        .collect();
                    if partitions.is_empty() {
                        // probably we're in the top layer. best add ourselves.
                        partitions.push(partition);
                    }
                    let partition_maxes: Vec<_> = partitions.iter().map(|p| p.len()).collect();

                    let choice_count =
                        std::cmp::min(neighborhood_size * 5, partition_maxes.iter().sum());
                    let partition_choices =
                        choose_n(choice_count, partition_maxes, node_id.0, &mut prng);

                    for i in 0..partition_choices.len() {
                        let partition = partitions[partition_choices[i].0];
                        let choice = &partition[partition_choices[i].1];
                        let distance = borrowed_comparator.compare_vec(
                            AbstractVector::Stored(*vector_id),
                            AbstractVector::Stored(choice.1),
                        );
                        distances.push((choice.0, distance));
                    }
                    distances.sort_by_key(|d| (OrderedFloat(d.1), d.0));
                    distances.dedup();
                    let (mut nodes, mut distances): (Vec<_>, Vec<f32>) = distances
                        .into_iter()
                        .filter(|(n, _d)| node_id != n)
                        .take(neighborhood_size)
                        .unzip();
                    //eprintln!("nodes@{node_id:?}: {nodes:?}");
                    nodes.resize_with(neighborhood_size, || NodeId(!0));
                    distances.resize_with(neighborhood_size, || f32::MAX);

                    let unsafe_nodes = unsafe {
                        std::slice::from_raw_parts_mut(
                            neighbors.as_ptr().add(node_id.0 * neighborhood_size) as *mut NodeId,
                            neighborhood_size,
                        )
                    };
                    let unsafe_distances = unsafe {
                        std::slice::from_raw_parts_mut(
                            neighbor_distances
                                .as_ptr()
                                .add(node_id.0 * neighborhood_size)
                                as *mut f32,
                            neighborhood_size,
                        )
                    };

                    unsafe_nodes.copy_from_slice(&nodes);
                    unsafe_distances.copy_from_slice(&distances);
                });
        });

        let neighbor_candidates: Vec<RwLock<PriorityQueue<NodeId>>> = neighbors
            .chunks_exact_mut(neighborhood_size)
            .zip(neighbor_distances.chunks_exact_mut(neighborhood_size))
            .map(|(data, priorities)| RwLock::new(PriorityQueue::from_slices(data, priorities)))
            .collect();

        eprintln!("Making neighborhoods bidirectional");
        // 4. Make neighborhoods bidirectional
        (0..neighbor_candidates.len())
            .into_par_iter()
            .for_each(|i| {
                let node = NodeId(i);
                let neighborhood_copy: Vec<(NodeId, f32)> = neighbor_candidates[i]
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|(n, _)| n.0 != !0)
                    .collect();
                //eprintln!("{neighborhood_copy:?}");
                for (neighbor, distance) in neighborhood_copy {
                    //eprintln!("inserting into: {}", neighbor.0);
                    neighbor_candidates[neighbor.0]
                        .write()
                        .unwrap()
                        .insert(node, distance);
                }
            });

        Layer {
            neighborhood_size,
            comparator,
            nodes: vs,
            neighbors,
        }
    }

    pub fn generate(
        c: C,
        mut vs: Vec<VectorId>,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> Self {
        // start with a shuffle
        let mut rng = thread_rng();
        vs.shuffle(&mut rng);
        //vs.sort();

        let total_size = vs.len();
        assert!(total_size > 0);
        // eprintln!("neighborhood_size: {neighborhood_size}");
        // eprintln!("total_size: {total_size}");
        // eprintln!("layer count: {layer_count}");
        // TODO: keeping a keepalive around for this entire generate is bad style!
        let _keep_alive = progress.keep_alive();
        let mut partitions = calculate_partitions(total_size, bp.order);
        eprintln!("generate with partitions: {partitions:?}");
        assert!(!partitions.is_empty());
        let layers = Vec::with_capacity(partitions.len());
        let neighborhood_size = bp.neighborhood_size;
        let zero_layer_neighborhood_size = bp.zero_layer_neighborhood_size;
        let mut hnsw: Hnsw<C> = Hnsw {
            layers,
            build_parameters: bp,
        };
        let mut i = 0;
        while i != partitions.len() {
            eprintln!("i: {i} len: {}", partitions.len());
            let layer_count = partitions.len();
            let length = partitions[i];
            let level = layer_count - i - 1;
            let slice_length = std::cmp::min(length, total_size);
            let slice = &vs[0..slice_length];
            let neighbors = if level == 0 {
                zero_layer_neighborhood_size
            } else {
                neighborhood_size
            };
            let layer = hnsw.generate_layer(
                c.clone(),
                slice.to_vec(),
                neighbors,
                bp.initial_partition_search,
                false,
            );
            hnsw.layers.push(layer);
            eprintln!("linking to better neighbors (during construction)");
            let old_layer_count = hnsw.layer_count();
            hnsw.improve_index(bp, None, progress);
            let new_layer_count = hnsw.layer_count();
            let delta = new_layer_count - old_layer_count;
            if delta > 0 {
                // new layers were added. We need to fix the partitions.
                let suffix: Vec<_> = partitions[i + 1..].to_vec();
                eprintln!("suffix: {suffix:?}");
                partitions = hnsw.layers.iter().map(|l| l.node_count()).collect();
                partitions.extend(suffix);
                eprintln!("recalculated new partitions: {partitions:?}");
                i += delta;
            }
            //hnsw.improve_neighbors(0.01, 0.01, None);
            i += 1;
        }

        hnsw
    }

    pub fn len(&self) -> usize {
        self.get_layer(0).unwrap().node_count()
    }

    pub fn is_empty(&self) -> bool {
        self.get_layer(0)
            .map(|l| l.nodes.is_empty())
            .unwrap_or(false)
    }

    pub fn knn(
        &self,
        k: usize,
        search_parameters: SearchParameters,
    ) -> impl ParallelIterator<Item = (VectorId, Vec<(VectorId, f32)>)> + '_ {
        let layer = &self.layers[self.layers.len() - 1];
        let nodes = &layer.nodes;

        nodes.par_iter().enumerate().map(move |(i, v)| {
            let node = NodeId(i);
            let abstract_vector = AbstractVector::Stored(*v);
            let eff_factor = 3;
            let mut pq = PriorityQueue::new(k * eff_factor);
            pq.merge_pairs(&[(node, 0.0)]);
            layer.closest_nodes(
                abstract_vector,
                &mut pq,
                search_parameters.number_of_candidates,
                search_parameters,
            );
            let distances: Vec<_> = pq
                .iter()
                .filter(|(n, _)| *n != node)
                .map(|(node_id, distance)| (layer.get_vector(node_id), distance))
                .take(k)
                .collect();
            (*v, distances)
        })
    }

    pub fn threshold_nn(
        &self,
        threshold: f32,
        search_parameters: SearchParameters,
    ) -> impl IndexedParallelIterator<Item = (VectorId, Vec<(VectorId, f32)>)> + '_ {
        let layer = &self.layers[self.layers.len() - 1];
        let nodes = &layer.nodes;

        nodes.par_iter().enumerate().map(move |(i, v)| {
            let node = NodeId(i);
            let abstract_vector = AbstractVector::Stored(*v);
            let mut pq = PriorityQueue::new(search_parameters.number_of_candidates);
            pq.merge_pairs(&[(node, 0.0)]);
            let mut last = 0.0;
            let mut last_size = 0;
            while last < threshold && pq.len() > last_size {
                last_size = pq.len();
                layer.closest_nodes(
                    abstract_vector.clone(),
                    &mut pq,
                    search_parameters.number_of_candidates,
                    search_parameters,
                );
                last = pq.last().expect("should have at least retrieved self").1;
                if last < threshold && pq.len() == pq.capacity() {
                    pq.resize_capacity(pq.capacity() * 2);
                }
            }
            let distances: Vec<_> = pq
                .iter()
                .filter(|(n, _)| *n != node)
                .take_while(|(_n, distance)| *distance < threshold)
                .map(|(node_id, distance)| (layer.get_vector(node_id), distance))
                .collect();
            (*v, distances)
        })
    }

    pub fn par_all_vectors(&self) -> impl ParallelIterator<Item = VectorId> + '_ {
        self.get_layer(0).unwrap().nodes.par_iter().cloned()
    }

    pub fn all_vectors(&self) -> AllVectorIterator {
        self.get_layer(0)
            .map(|layer| {
                let iter = layer.nodes.iter();
                AllVectorIterator::Full { iter }
            })
            .unwrap_or(AllVectorIterator::Empty)
    }

    pub fn supers_for_layer(&self, layer_id: usize) -> &[VectorId] {
        if self.layer_count() == layer_id + 1 {
            let layer = self.get_layer(layer_id).unwrap();
            &layer.nodes[0..1]
        } else {
            &self.get_layer(layer_id + 1).unwrap().nodes[..]
        }
    }

    pub fn node_distances_for_layer(&self, layer_id: usize) -> Vec<NodeDistance> {
        let layer = self.get_layer(layer_id).unwrap();
        let supers = self.supers_for_layer(layer_id);
        layer.node_distances(supers)
    }

    pub fn reachables_from_node_for_layer(
        &self,
        layer_id: usize,
        node: NodeId,
        check: &[NodeId],
    ) -> Vec<(NodeId, usize)> {
        let layer = self.get_layer_from_top(layer_id).unwrap();
        layer.reachables_from(node, check)
    }

    pub fn discover_unreachable_vectors(
        &self,
        layer_id_from_top: usize,
        search_parameters: SearchParameters,
    ) -> Vec<VectorId> {
        //const THRESHOLD: usize = 42;

        let layers = &self.layers[0..=layer_id_from_top];
        let layer_above = if layer_id_from_top == 0 {
            None
        } else {
            Some(&layers[layer_id_from_top - 1])
        };
        eprintln!("layer count for discovery: {}", layers.len());
        let current_layer = &layers[layer_id_from_top];
        current_layer
            .nodes
            .par_iter()
            .filter_map(|vector| {
                let (matches, _index_distance) = search::search_layers_instrumented(
                    AbstractVector::Stored(*vector),
                    search_parameters,
                    layers,
                    None,
                );
                let vector_is_in_matches = match_within_epsilon(*vector, matches);
                if !vector_is_in_matches
                    && (layer_above.is_none() || layer_above.unwrap().get_node(*vector).is_none())
                {
                    Some(*vector)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn extend_layer(&mut self, layer_id: usize, vecs: Vec<VectorId>) {
        let layer_id_from_top = self.layer_count() - layer_id - 1;
        eprintln!("Extending layer: {layer_id:?}");
        eprintln!("Counting as {layer_id_from_top:?}");
        let (layers_above, layers_below) = self.layers.split_at_mut(layer_id_from_top);
        eprintln!(
            "layers above: {}, layers_below: {}",
            layers_above.len(),
            layers_below.len()
        );
        let layer = &mut layers_below[0];

        let (old_nodes, old_nodes_map, new_nodes_map, _vecs) = generate_node_maps(vecs, layer);

        eprintln!("created node maps");
        assert_eq!(old_nodes_map.len() + new_nodes_map.len(), layer.nodes.len());

        let new_neighbors_len = layer.nodes.len() * layer.neighborhood_size;
        let mut old_neighbors = Vec::with_capacity(new_neighbors_len);
        #[allow(clippy::uninit_vec)]
        unsafe {
            old_neighbors.set_len(new_neighbors_len)
        };
        eprintln!("allocated new neighbors");
        std::mem::swap(&mut layer.neighbors, &mut old_neighbors);

        copy_old_neighborhoods_into_layer(&old_nodes, &old_neighbors, &old_nodes_map, layer);
        eprintln!("copied old neighbors into new layer");
        initialize_new_neighborhoods_into_layer(&new_nodes_map, layer);
    }

    pub fn link_layer_to_better_neighbors(
        &mut self,
        layer_from_top: usize,
        sp: SearchParameters,
    ) -> usize {
        self.link_nodes_in_layer_to_better_neighbors(
            layer_from_top,
            (0..self.get_layer_from_top(layer_from_top).unwrap().nodes.len())
                .into_par_iter()
                .map(NodeId),
            sp,
        )
    }

    pub fn link_nodes_in_layer_to_better_neighbors<
        I: ParallelIterator<Item = NodeId>,
        II: IntoParallelIterator<Iter = I, Item = NodeId>,
    >(
        &mut self,
        layer_from_top: usize,
        nodes: II,
        sp: SearchParameters,
    ) -> usize {
        let neighborhood_size = self.neighborhood_size();
        let (top_stack, bottom_stack) = self.layers.split_at_mut(layer_from_top);
        let current_layer = &mut bottom_stack[0];

        let pseudo_layer = current_layer.clone();
        let mut pseudo_stack: Vec<&Layer<C>> = Vec::with_capacity(top_stack.len() + 1);
        pseudo_stack.extend(top_stack.iter());
        pseudo_stack.push(&pseudo_layer);

        let neighbor_candidates: Vec<RwLock<&mut [NodeId]>> = current_layer
            .neighbors
            .chunks_exact_mut(current_layer.neighborhood_size)
            .map(RwLock::new)
            .collect();
        nodes
            .into_par_iter()
            .map(|local_node| {
                let mut count = 0;
                let vector = pseudo_layer.get_vector(local_node);
                let matches = search::search_layers(
                    AbstractVector::Stored(vector),
                    sp,
                    &pseudo_stack,
                    Some(vector),
                );
                for (neighbor_vec, distance) in matches.into_iter().take(neighborhood_size) {
                    if neighbor_vec == vector {
                        break;
                    }
                    let neighbor = pseudo_layer.get_node(neighbor_vec).unwrap();
                    let neighbor_of_neighbors = neighbor_candidates[neighbor.0].read().unwrap();
                    if let Some(insert_pos) = neighbor_of_neighbors.iter().position(|n| {
                        if n.is_empty() || *n == local_node {
                            return true;
                        }
                        let v = pseudo_layer.get_vector(*n);
                        let other_distance = current_layer.comparator.compare_vec(
                            AbstractVector::Stored(v),
                            AbstractVector::Stored(neighbor_vec),
                        );
                        distance < other_distance || (distance == other_distance && local_node < *n)
                    }) {
                        if neighbor_of_neighbors[insert_pos] == local_node {
                            // we are already in this neighbor list. do not add again
                            continue;
                        }
                        std::mem::drop(neighbor_of_neighbors);
                        let mut neighbor_of_neighbors =
                            neighbor_candidates[neighbor.0].write().unwrap();

                        for i in (insert_pos..neighbor_of_neighbors.len() - 1).rev() {
                            neighbor_of_neighbors[i + 1] = neighbor_of_neighbors[i];
                        }
                        neighbor_of_neighbors[insert_pos] = local_node;
                        count += 1;
                    }
                }

                count
            })
            .sum()
    }

    fn improve_neighborhoods_at_layer(
        &mut self,
        layer_from_top: usize,
        sp: SearchParameters,
    ) -> usize {
        eprintln!("improving {layer_from_top}");
        let count = self.link_layer_to_better_neighbors(layer_from_top, sp);
        eprintln!("{layer_from_top}: relinked {count}");
        count
    }

    fn discover_order_from_top(&self, v: VectorId) -> usize {
        for (i, l) in self.layers.iter().enumerate() {
            if l.get_node(v).is_some() {
                return i;
            }
        }
        panic!("Vector {v:?} does not exist in hnsw!");
    }

    fn filter_promotion_candidates(
        &self,
        layer_from_top: usize,
        vecs: Vec<VectorId>,
        search_parameters: SearchParameters,
    ) -> Vec<(usize, Vec<VectorId>)> {
        if layer_from_top == 0 {
            return Vec::new();
        }
        let mut histomap: HashMap<usize, HashMap<NodeId, usize>> = HashMap::new();
        // - do histogramming
        eprintln!("generate histogram");
        let mut vecs = vecs.clone();
        vecs.sort();
        for v in vecs.iter() {
            let order = self.discover_order_from_top(*v);
            if order == 0 {
                continue;
            }
            let order_layer = self.get_layer_from_top(order).unwrap();
            let node = order_layer.get_node(*v).unwrap();
            let neighbors = order_layer.get_neighbors(node);
            match histomap.entry(order) {
                Entry::Occupied(mut layer_histo_entry) => {
                    let layer_histo = layer_histo_entry.get_mut();
                    for n in neighbors {
                        let neighbor_vector = order_layer.get_vector(*n);
                        if vecs.binary_search(&neighbor_vector).is_ok() {
                            match layer_histo.entry(*n) {
                                Entry::Occupied(mut o) => *o.get_mut() += 1,
                                Entry::Vacant(o) => {
                                    o.insert(1);
                                }
                            }
                        }
                    }
                }
                Entry::Vacant(o) => {
                    let mut layer_histo = HashMap::new();
                    for n in neighbors {
                        let neighbor_vector = order_layer.get_vector(*n);
                        if vecs.binary_search(&neighbor_vector).is_ok() {
                            layer_histo.insert(*n, 1);
                        }
                    }
                    o.insert(layer_histo);
                }
            }
        }
        let mut order_histograms: Vec<(usize, Vec<(NodeId, usize)>)> = histomap
            .into_iter()
            .map(|(order, h)| {
                let mut histo: Vec<(NodeId, usize)> = h.into_iter().collect();
                histo.sort_by_key(|(_, count)| *count);
                (order, histo)
            })
            .collect();
        // - pick top histogrammed
        order_histograms.sort_by_key(|(order, _)| *order);
        //eprintln!("Order histos: {order_histograms:?}");
        let mut result = Vec::new();
        for (order, mut histogram) in order_histograms {
            eprintln!("order: {order}");
            let layer = self.get_layer_from_top(order).unwrap();
            eprintln!("layer node count: {}", layer.node_count());
            let mut promotion_selection = Vec::new();
            eprintln!("find promotions (out of {})", histogram.len());
            while let Some((node, _)) = histogram.pop() {
                if promotion_selection.par_iter().any(|(v, radius)| {
                    self.comparator().compare_vec(
                        AbstractVector::Stored(*v),
                        AbstractVector::Stored(layer.get_vector(node)),
                    ) < *radius
                }) {
                    continue;
                }

                // - figure out distance as supernode
                let vec = layer.get_vector(node);
                let result = self.search_upto(
                    AbstractVector::Stored(vec),
                    search_parameters,
                    layer_from_top,
                );
                let radius = result[0].1; // todo gotta tune this hypersphere
                promotion_selection.push((vec, radius));
            }
            result.push((
                order,
                promotion_selection.into_iter().map(|(v, _)| v).collect(),
            ));
        }
        eprintln!("done finding promotions");

        result
    }

    pub fn promote_at_layer(
        &mut self,
        layer_from_top: usize,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> bool {
        let max_proportion = bp.optimization.promotion_proportion;
        eprintln!("promoting batch at layer from top: {layer_from_top}");
        let mut vecs = self.discover_unreachable_vectors(layer_from_top, bp.optimization.search);
        for i in 0..self.layer_count() {
            eprintln!("{}: Layer size is: {}", i, self.layers[i].node_count());
        }
        if vecs.is_empty() {
            return false;
        }
        if max_proportion < 1.0 {
            let vec_length = (vecs.len() as f32 * max_proportion) as usize;
            vecs.truncate(vec_length);
            // check after proportion
            if vecs.is_empty() {
                return false;
            }
        }
        let order_vecs =
            self.filter_promotion_candidates(layer_from_top, vecs, bp.optimization.search);
        for (layer_from_top, mut vecs) in order_vecs {
            eprintln!(
                "vec len for promotion: {}@{} (out of {})",
                vecs.len(),
                layer_from_top,
                self.get_layer_from_top(layer_from_top)
                    .unwrap()
                    .node_count()
            );
            if layer_from_top == 0 {
                // just construct a new hnsw and copy over layer stack
                // we don't even care about the vectors to promote we found previously.
                let layer = self.get_layer_from_top(0).unwrap();
                let old_nodes = layer.nodes.clone();
                vecs.extend(old_nodes);
                vecs.sort();
                // shouldn't do anything
                vecs.dedup();
                // Reuse parameters, but our zero layer is not a real zero.
                let mut new_bp = bp;
                new_bp.zero_layer_neighborhood_size = bp.neighborhood_size;
                let new_top = Self::generate(self.comparator().clone(), vecs, new_bp, progress);
                let mut layers = new_top.layers;
                eprintln!(
                    "generated {} new top layers (and nothing else)",
                    layers.len()
                );
                std::mem::swap(&mut self.layers, &mut layers);
                self.layers.extend(layers);
                for layer in self.layers.iter() {
                    eprintln!("layer count: {}", layer.node_count());
                }
            } else {
                let mut sizes: Vec<_> = self
                    .layers
                    .iter()
                    .take(layer_from_top)
                    .map(|l| l.node_count())
                    .collect();
                sizes.reverse();
                eprintln!("sizes: {sizes:?}");
                let mut new_sizes = calculate_partitions_from_bottom(
                    sizes.first().unwrap() + vecs.len(),
                    self.build_parameters.order,
                );
                eprintln!("new sizes: {new_sizes:?}");
                if new_sizes.len() < sizes.len() {
                    // apparently our current stack is pretty suboptimal. That is bound to happen after a couple of promotions.
                    // our 'solution' is just to pad new_sizes with zeroes. The ssaturating sub below will ensure that this results in 0 promotions into these unbalanced top layers.
                    new_sizes.resize(sizes.len(), 0);
                }

                let retop_upto = new_sizes.len() - sizes.len();
                new_sizes.truncate(sizes.len());
                eprintln!("truncated new: {new_sizes:?}");

                let mut promotion_sizes: Vec<_> = new_sizes
                    .into_iter()
                    .zip(sizes)
                    .map(|(s1, s2)| s1.saturating_sub(s2))
                    .collect();
                eprintln!("promotion sizes: {promotion_sizes:?}");
                let offset = if retop_upto != 0 {
                    // this briefly breaks the invariant about nodes
                    // always existing in layers below.
                    let retop_index_in_promotions = promotion_sizes.len() - retop_upto;
                    let promotion_into_top = promotion_sizes[retop_index_in_promotions];
                    promotion_sizes.truncate(retop_index_in_promotions);
                    //let promotion_into_top = promotion_sizes.pop().unwrap();
                    let mut top_vecs = self
                        .get_layer_from_top(retop_upto - 1)
                        .unwrap()
                        .nodes
                        .clone();
                    top_vecs.extend(vecs[..promotion_into_top].iter().copied());
                    //let mut rng = thread_rng();
                    //top_vecs.shuffle(&mut rng);
                    top_vecs.sort();
                    top_vecs.dedup();

                    // Reuse build parameters but with a fake zero layer
                    let mut new_bp = bp;
                    new_bp.zero_layer_neighborhood_size = bp.neighborhood_size;

                    let new_top = Self::generate(
                        self.comparator().clone(),
                        top_vecs.clone(),
                        new_bp,
                        progress,
                    );
                    let layers = new_top.layers;
                    let new_top_len = layers.len();
                    eprintln!("generated {} new top layers (and extending)", layers.len());
                    // having merged the top layer with the promotions, we can throw away the original.
                    let suffix = self.layers[retop_upto..].to_vec();
                    self.layers.clear();
                    self.layers.extend(layers);
                    self.layers.extend(suffix);
                    for (i, layer) in self.layers.iter().enumerate() {
                        eprintln!("{i}: layer node count: {}", layer.node_count());
                    }
                    new_top_len // note, we already popped one off promotion_sizes, so we want to start /after/ the new top
                } else {
                    0 // we have not generated a new top, and therefore the first extend starts at layer 0
                };
                promotion_sizes.reverse();
                for (i, size) in promotion_sizes.into_iter().enumerate() {
                    let current_layer_from_top = offset + i;
                    eprintln!("layer_id_from_top: {current_layer_from_top}");
                    let layer = self.get_layer_from_top(current_layer_from_top).unwrap();
                    let vecs_to_promote: Vec<_> = vecs
                        .iter()
                        .cloned()
                        .filter(|v| layer.get_node(*v).is_none())
                        .take(size)
                        .collect();
                    let current_layer_from_bottom =
                        self.layer_from_top_to_layer(current_layer_from_top);
                    self.extend_layer(current_layer_from_bottom, vecs_to_promote);
                }

                eprintln!("after extension");
                for i in 0..self.layer_count() {
                    eprintln!("{}: Layer size is: {}", i, self.layers[i].node_count());
                }
                assert_layer_invariants(&self.layers);
            }
        }
        true
    }

    /*
    #[allow(unused)]
    fn promote_at_layer(&mut self, layer_from_top: usize) -> (Vec<VectorId>, usize) {
        let mut vecs = self.discover_vectors_to_promote_2(layer_from_top);
        vecs.sort();
        let count = vecs.len();
        eprintln!("layer_from_top {layer_from_top}: promoting {count} vecs");
        //eprintln!("Vectors to promote: {vecs:?}");
        if count != 0 {
            if layer_from_top == 0 {
                // generate new layer(s)
                let new_top = Self::generate(
                    self.comparator().clone(),
                    vecs.clone(),
                    self.neighborhood_size,
                    self.neighborhood_size,
                    self.order,
                );
                let mut layers = new_top.layers;
                eprintln!("generated {} new top layers", layers.len());
                std::mem::swap(&mut self.layers, &mut layers);
                self.layers.extend(layers);
                return (vecs, 0);
            } else {
                // extend existing layer
                let layer_above = self.layer_from_top_to_layer(layer_from_top - 1);
                self.extend_layer(layer_above, vecs.clone());
                return (vecs, layer_from_top - 1);
            }
        }

        (vecs, layer_from_top)
    }
    */
    pub fn stochastic_recall_at(
        &self,
        at: usize,
        optimization_parameters: OptimizationParameters,
    ) -> f32 {
        let mut rng = StdRng::seed_from_u64(42);
        eprintln!("stochastic recall at: {at}");
        eprintln!("layer count: {}", self.layer_count());
        let layer = self.get_layer_from_top(at).unwrap();
        let total = layer.node_count();
        let selection = usize::max(
            1,
            (total as f32 * optimization_parameters.recall_proportion) as usize,
        );
        let vecs_to_find: Vec<VectorId> = if selection == total {
            layer.nodes.clone()
        } else {
            let mut vecs = layer.nodes.clone();
            vecs.shuffle(&mut rng);
            vecs.truncate(selection);
            vecs
        };
        let relevant: usize = vecs_to_find
            .par_iter()
            .filter(|vid| {
                let res = self.search(
                    AbstractVector::Stored(**vid),
                    optimization_parameters.search,
                );
                res.iter().map(|(v, _)| v).any(|v| v == *vid)
            })
            .count();

        let recall = relevant as f32 / selection as f32;
        eprintln!("with recall: {recall}");
        recall
    }

    pub fn stochastic_recall(&self, optimization_parameters: OptimizationParameters) -> f32 {
        eprintln!("In stochastic recall");
        assert!(self.layer_count() != 0);
        self.stochastic_recall_at(self.layer_count() - 1, optimization_parameters)
    }

    pub fn improve_neighbors(
        &mut self,
        op: OptimizationParameters,
        last_recall: Option<f32>,
    ) -> f32 {
        self.improve_neighbors_upto(self.layer_count(), op, last_recall)
    }

    pub fn improve_neighbors_upto(
        &mut self,
        upto: usize,
        op: OptimizationParameters,
        last_recall: Option<f32>,
    ) -> f32 {
        assert!(upto <= self.layer_count());
        assert!(upto >= 1);
        let neighborhood_threshold = op.neighborhood_threshold;

        let mut last_recall = last_recall.unwrap_or(0.0);
        let mut last_improvement = 1.0_f32;
        while last_improvement >= neighborhood_threshold && last_recall < 1.0 {
            for layer_id_from_top in 0..upto {
                let count = self.improve_neighborhoods_at_layer(layer_id_from_top, op.search);
                eprintln!("layer {layer_id_from_top}: improved {count}");
            }

            let recall = self.stochastic_recall_at(upto - 1, op);
            last_improvement = recall - last_recall;
            last_recall = recall;
            eprintln!(
                "recall at {}/{} {recall} (improvement: {last_improvement})",
                upto,
                self.layer_count()
            );
        }

        last_recall
    }

    pub fn improve_index_at(
        &mut self,
        mut layer_from_top: usize,
        bp: BuildParameters,
        last_recall: Option<f32>,
        progress: &mut dyn ProgressMonitor,
    ) -> (f32, usize) {
        // TODO: holding a guard for the duration of the entire function is bad style!
        let _guard = progress.keep_alive();
        let op = bp.optimization;
        let promotion_threshold = op.promotion_threshold;

        // let's start with a neighborhood optimization so we don't overpromote
        let mut recall = last_recall
            .unwrap_or_else(|| self.stochastic_recall_at(layer_from_top, bp.optimization));

        let mut improvement = 1.0;
        let mut bailout = 1;
        let mut current_layer_from_top;
        while improvement >= promotion_threshold && recall < 1.0 && bailout != 0 {
            eprintln!("improve_index_at {improvement}>={promotion_threshold} && {recall} < 1.0 && {bailout} != 0");
            let last_recall = recall;
            current_layer_from_top = 0;
            while current_layer_from_top <= layer_from_top && bailout != 0 {
                let layer_count = self.layer_count();
                eprintln!("improve_index_at is going to call improve_neighbors_upto");
                recall = self.improve_neighbors_upto(current_layer_from_top + 1, op, None);

                if recall == 1.0 {
                    current_layer_from_top += 1;
                    continue;
                }

                eprintln!("About to promote");
                if self.promote_at_layer(current_layer_from_top, bp, progress) {
                    let new_layer_count = self.layer_count();
                    eprintln!("New layer count: {new_layer_count}, old layer count: {layer_count}");
                    let layer_delta = new_layer_count - layer_count;
                    assert!(new_layer_count >= layer_count);
                    eprintln!("We did promote!  With layer delta: {layer_delta}");
                    eprintln!("original current_layer_from_top is {current_layer_from_top}");
                    current_layer_from_top += layer_delta;
                    layer_from_top += layer_delta;
                    eprintln!("corrected current_layer_from_top is {current_layer_from_top}");
                    recall =
                        self.improve_neighbors_upto(current_layer_from_top + 1, op, Some(recall));
                    eprintln!("recall after promotion: {recall}");
                }

                current_layer_from_top += 1;
            }
            bailout -= 1;
            improvement = recall - last_recall;
            eprintln!("outer loop improvement: {improvement}");
        }

        (recall, layer_from_top)
    }

    /*
       pub fn improve_index_at(
           &mut self,
           layer_from_top: usize,
           promotion_threshold: f32,
           neighbor_threshold: f32,
           recall_proportion: f32,
           promotion_proportion: f32,
           last_recall: Option<f32>,
           progress: &mut dyn ProgressMonitor,
       ) -> (f32, usize) {
           let _guard = progress.keep_alive();
           // let's start with a neighborhood optimization so we don't overpromote
           let mut recall = last_recall
               .unwrap_or_else(|| self.stochastic_recall_at(layer_from_top, recall_proportion));

           let mut improvement = 1.0;
           let mut bailout = 1;
           let mut current_layer_from_top;
           while improvement >= promotion_threshold && recall < 1.0 && bailout != 0 {
               eprintln!("improve_index_at {improvement}>={promotion_threshold} && {recall} < 1.0 && {bailout} != 0");
               let last_recall = recall;
               current_layer_from_top = 0;
               while current_layer_from_top <= layer_from_top && bailout != 0 {
                   eprintln!("improve_index_at is going to call improve_neighbors_upto");
                   recall = self.improve_neighbors_upto(
                       current_layer_from_top + 1,
                       neighbor_threshold,
                       recall_proportion,
                       None,
                   );

                   eprintln!("About to promote");
                   let mut delta = 0;
                   let layer_count = self.layer_count();
                   if self.promote_at_layer(layer_from_top, promotion_proportion) {
                       eprintln!("corrected current_layer_from_top is {current_layer_from_top}");
                       delta = self.layer_count() as isize - layer_count as isize;
                       recall = self.improve_neighbors_upto(
                           self.layer_count(),
                           neighbor_threshold,
                           recall_proportion,
                           Some(recall),
                       );
                       eprintln!("recall after promotion: {recall}");
                   }

                   current_layer_from_top = (current_layer_from_top as isize + 1 + delta) as usize;
               }

               bailout -= 1;
               improvement = recall - last_recall;
               eprintln!("outer loop improvement: {improvement}");
           }

           (recall, layer_from_top)
       }
    */

    pub fn improve_index(
        &mut self,
        bp: BuildParameters,
        last_recall: Option<f32>,
        progress: &mut dyn ProgressMonitor,
    ) -> f32 {
        // let's start with a neighborhood optimization so we don't overpromote
        let mut recall = last_recall.unwrap_or_else(|| self.stochastic_recall(bp.optimization));

        let mut layer_from_top = 0;
        while layer_from_top < self.layer_count() {
            eprintln!(
                "improve index, layer_from_top={layer_from_top}, layer_count: {}",
                self.layer_count()
            );
            (recall, layer_from_top) = self.improve_index_at(layer_from_top, bp, None, progress);
            eprintln!("afterwards, layer_from_top is {layer_from_top}");
            layer_from_top += 1;
        }

        recall
    }
}

impl<C: Serializable + Clone> Hnsw<C> {
    pub fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        serialize::serialize_hnsw(self.build_parameters, &self.layers, path)
    }

    pub fn deserialize<P: AsRef<Path>>(
        path: P,
        params: C::Params,
    ) -> Result<Self, SerializationError> {
        serialize::deserialize_hnsw(path, params)
    }
}

#[allow(unused)]
fn cross_compare_vectors<C: Comparator + 'static>(
    vecs: &Vec<VectorId>,
    borrowed_comparator: &C,
) -> HashMap<VectorId, Vec<(VectorId, f32)>> {
    let cross_compare: HashMap<VectorId, Vec<(VectorId, f32)>> = vecs
        .par_iter()
        .map(|v| {
            let distances: Vec<(VectorId, f32)> = vecs
                .par_iter()
                .flat_map(|w| {
                    if w == v {
                        None
                    } else {
                        let distance = borrowed_comparator
                            .compare_vec(AbstractVector::Stored(*w), AbstractVector::Stored(*v));
                        Some((*w, distance))
                    }
                })
                .collect();
            (*v, distances)
        })
        .collect();
    cross_compare
}

fn initialize_new_neighborhoods_into_layer<C: Comparator + 'static>(
    new_nodes_map: &[usize],
    layer: &mut Layer<C>,
) {
    for node in new_nodes_map {
        let neighbors = layer.get_neighborhood_mut(NodeId(*node));
        neighbors.fill(NodeId(!0));
    }
}

fn copy_old_neighborhoods_into_layer<C: Comparator + 'static>(
    old_nodes: &[VectorId],
    old_neighbors: &[NodeId],
    old_nodes_map: &[usize],
    layer: &mut Layer<C>,
) {
    // insert old nodes with shifted offsets
    (0..old_nodes.len())
        .into_par_iter()
        .for_each(|old_node_id| {
            let nhs = layer.neighborhood_size;
            let old_neighborhood = &old_neighbors[old_node_id * nhs..(old_node_id + 1) * nhs];
            let new_node_id = old_nodes_map[old_node_id];
            let new_neighborhood = unsafe {
                std::slice::from_raw_parts_mut(
                    layer.neighbors.as_ptr().add(new_node_id * nhs) as *mut NodeId,
                    nhs,
                )
            };

            for i in 0..nhs {
                if old_neighborhood[i].0 == !0 {
                    new_neighborhood[i] = NodeId(!0);
                } else {
                    new_neighborhood[i] = NodeId(old_nodes_map[old_neighborhood[i].0]);
                }
            }
        });
}

fn generate_node_maps<C: Comparator + 'static>(
    mut vecs: Vec<VectorId>,
    layer: &mut Layer<C>,
) -> (Vec<VectorId>, Vec<usize>, Vec<usize>, Vec<VectorId>) {
    vecs.sort();
    // this will be swapped out for the current nodes, therefore the variable name
    let mut old_nodes = Vec::with_capacity(vecs.len() + layer.nodes.len());
    std::mem::swap(&mut old_nodes, &mut layer.nodes);

    let mut old_nodes_iter = old_nodes.iter().peekable();
    let mut new_nodes_iter = vecs.iter().peekable();
    let mut old_nodes_map = Vec::with_capacity(old_nodes.len());
    let mut new_nodes_map = Vec::with_capacity(vecs.len());
    let mut vecs: Vec<VectorId> = Vec::with_capacity(vecs.len());
    loop {
        let old_nodes_next = old_nodes_iter.peek();
        let new_nodes_next = new_nodes_iter.peek();
        match (old_nodes_next, new_nodes_next) {
            (None, None) => break,
            (Some(_), None) => {
                layer.nodes.push(*old_nodes_iter.next().unwrap());
                old_nodes_map.push(layer.nodes.len() - 1);
            }
            (None, Some(v)) => {
                vecs.push(**v);
                layer.nodes.push(*new_nodes_iter.next().unwrap());
                new_nodes_map.push(layer.nodes.len() - 1);
            }
            (Some(old), Some(new)) => match old.cmp(new) {
                std::cmp::Ordering::Equal => {
                    panic!("tried to insert vector that already exists in this layer")
                }
                std::cmp::Ordering::Less => {
                    layer.nodes.push(*old_nodes_iter.next().unwrap());
                    old_nodes_map.push(layer.nodes.len() - 1);
                }
                std::cmp::Ordering::Greater => {
                    vecs.push(**new);
                    layer.nodes.push(*new_nodes_iter.next().unwrap());
                    new_nodes_map.push(layer.nodes.len() - 1);
                }
            },
        }
    }
    (old_nodes, old_nodes_map, new_nodes_map, vecs)
}

pub enum AllVectorIterator<'a> {
    Full { iter: Iter<'a, VectorId> },
    Empty,
}

impl<'a> Iterator for AllVectorIterator<'a> {
    type Item = VectorId;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            AllVectorIterator::Full { iter } => iter.next().cloned(),
            AllVectorIterator::Empty => None,
        }
    }
}

fn choose_n_1(
    n: usize,
    partition_maxes: Vec<usize>,
    exclude: usize,
    prng: &mut StdRng,
) -> Vec<(usize, usize)> {
    let mut result: Vec<(usize, usize)> = (0..partition_maxes.len())
        .flat_map(|partition| {
            (0..partition_maxes[partition]).filter_map(move |in_partition| {
                if partition == 0 && in_partition == exclude {
                    None
                } else {
                    Some((partition, in_partition))
                }
            })
        })
        .collect();
    result.shuffle(prng);

    result.truncate(n);

    result
}

fn choose_n(
    n: usize,
    partition_maxes: Vec<usize>,
    exclude: usize,
    prng: &mut StdRng,
) -> Vec<(usize, usize)> {
    // todo: probably should give higher chance to select our own partition
    if partition_maxes.iter().sum::<usize>() * 2 > n {
        return choose_n_1(n, partition_maxes, exclude, prng);
    }

    let mut count = 0;
    let mut set = HashSet::with_capacity(n);
    let exp = Exp::new(1.0_f32).unwrap();
    while count != n {
        let mut which_partition = exp.sample(prng).floor() as usize;
        if which_partition >= partition_maxes.len() {
            which_partition = 0;
        }
        let selection = prng.gen_range(0..partition_maxes[which_partition]);
        if (which_partition != 0 || selection != exclude)
            && set.insert((which_partition, selection))
        {
            count += 1;
        }
    }
    set.into_iter().collect()
}

fn calculate_partitions_from_bottom(total_size: usize, order: usize) -> Vec<usize> {
    let mut partitions: Vec<usize> = vec![];
    let mut size = total_size;
    let layer_count = usize::max(1, (total_size as f32).log(order as f32).ceil() as usize);
    for _ in 0..layer_count {
        partitions.push(size);
        size /= order;
    }

    partitions
}

fn calculate_partitions(total_size: usize, order: usize) -> Vec<usize> {
    let mut partitions = calculate_partitions_from_bottom(total_size, order);
    partitions.reverse();
    partitions
}

#[allow(unused)]
fn calculate_partitions_for_additions(
    sizes_from_bottom: &[usize],
    new_vecs: usize,
    order: usize,
) -> Vec<usize> {
    let mut new_partitions =
        calculate_partitions_from_bottom(sizes_from_bottom[0] + new_vecs, order);
    // ensure we have at least as many partitions as existing sizes (but maybe more)
    if new_partitions.len() < sizes_from_bottom.len() {
        new_partitions.resize(sizes_from_bottom.len(), 0);
    }
    eprintln!("new partitions: {new_partitions:?}\nsizes: {sizes_from_bottom:?}");

    let mut new_sizes: Vec<usize> = new_partitions
        .into_iter()
        .enumerate()
        .map(|(ix, p)| {
            if ix >= sizes_from_bottom.len() {
                p
            } else {
                usize::max(p, sizes_from_bottom[ix])
            }
        })
        .collect();

    // Ensure that the new layer stack is monotonic.
    // We have to make sure that we don't cause a super layer to grow larger than its sublayers.
    let mut last = 0_usize;
    for elt in new_sizes.iter_mut().rev() {
        if last > *elt {
            *elt = last;
        }
        last = *elt;
    }
    eprintln!("corrected partitions: {new_sizes:?}");

    let mut partition_additions: Vec<usize> = new_sizes
        .into_iter()
        .enumerate()
        .map(|(ix, p)| {
            if ix >= sizes_from_bottom.len() {
                p
            } else {
                p - sizes_from_bottom[ix]
            }
        })
        .collect();

    // ensure that promotions are reverse monotonic.
    // We have to make sure that if we promote at a superlayer, it is also promoted at the sublayer.
    let mut last = 0_usize;
    for elt in partition_additions.iter_mut().rev() {
        if last > *elt {
            *elt = last;
        }
        last = *elt;
    }
    partition_additions
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use rand_distr::Uniform;

    use super::bigvec::*;
    use super::*;
    type SillyVec = [f32; 3];
    #[derive(Clone, Debug, PartialEq)]
    struct SillyComparator {
        data: Vec<SillyVec>,
    }

    impl Comparator for SillyComparator {
        type T = SillyVec;
        type Borrowable<'a> = &'a SillyVec;

        fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
            &self.data[v.0]
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            let mut result = 0.0;
            for (&f1, &f2) in v1.iter().zip(v2.iter()) {
                result += f1 * f2
            }
            1.0 - result
        }
    }

    fn make_simple_hnsw() -> Hnsw<SillyComparator> {
        let sqrt2_recip = std::f32::consts::FRAC_1_SQRT_2;
        let data: Vec<SillyVec> = vec![
            [1.0, 0.0, 0.0],                 // 0
            [0.0, 1.0, 0.0],                 // 1
            [0.0, 0.0, 1.0],                 // 2
            [sqrt2_recip, sqrt2_recip, 0.0], // 3
            [0.5773, 0.5773, 0.5773],        // 4
            [-1.0, 0.0, 0.0],                // 5
            [0.0, -1.0, 0.0],                // 6
            [0.0, 0.0, -1.0],                // 7
            [0.0, sqrt2_recip, sqrt2_recip], // 8
        ];
        let c = SillyComparator { data: data.clone() };
        let vs: Vec<_> = (0..9).map(VectorId).collect();
        let mut bp = BuildParameters::default();
        bp.order = 6;
        bp.neighborhood_size = 3;
        bp.zero_layer_neighborhood_size = 6;
        let hnsw: Hnsw<SillyComparator> = Hnsw::generate(c, vs, bp, &mut ());
        hnsw
    }

    fn make_broken_hnsw() -> Hnsw<SillyComparator> {
        let sqrt2_recip = std::f32::consts::FRAC_1_SQRT_2;
        let data: Vec<SillyVec> = vec![
            [1.0, 0.0, 0.0],                 // 0
            [0.0, 1.0, 0.0],                 // 1
            [0.0, 0.0, 1.0],                 // 2
            [sqrt2_recip, sqrt2_recip, 0.0], // 3
            [0.5773, 0.5773, 0.5773],        // 4
            [-1.0, 0.0, 0.0],                // 5
            [0.0, -1.0, 0.0],                // 6
            [0.0, 0.0, -1.0],                // 7
            [0.0, sqrt2_recip, sqrt2_recip], // 8
            [sqrt2_recip, 0.0, sqrt2_recip], // 9
        ];
        let c = SillyComparator { data: data.clone() };
        let vs: Vec<_> = (0..9).map(VectorId).collect(); // only index 8 first..
        let mut bp = BuildParameters::default();
        bp.order = 6;
        bp.neighborhood_size = 3;
        bp.zero_layer_neighborhood_size = 6;

        let mut hnsw: Hnsw<SillyComparator> = Hnsw::generate(c, vs, bp, &mut ());
        let bottom = &mut hnsw.layers[1];
        // add a ninth disconnected vector
        bottom.nodes.push(VectorId(9));
        bottom.neighbors.extend(vec![NodeId(!0); 6]);
        hnsw
    }

    #[test]
    fn test_nearness_search() {
        let hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        let sqrt2_recip = std::f32::consts::FRAC_1_SQRT_2;
        let slice = &[0.0, sqrt2_recip, sqrt2_recip];
        let search_vector = AbstractVector::Unstored(slice);

        let results = hnsw.search(search_vector, hnsw.build_parameters.optimization.search);
        assert_eq!(
            results,
            vec![
                (VectorId(8), 5.9604645e-8),
                (VectorId(4), 0.1835745),
                (VectorId(1), 0.29289323),
                (VectorId(2), 0.29289323),
                (VectorId(3), 0.5),
                (VectorId(0), 1.0),
                (VectorId(5), 1.0),
                (VectorId(6), 1.7071068),
                (VectorId(7), 1.7071068)
            ],
        )
    }

    #[test]
    fn test_generation() {
        let hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        assert_eq!(
            hnsw.get_layer(0).map(|layer| &layer.nodes),
            Some(
                vec![
                    VectorId(0),
                    VectorId(1),
                    VectorId(2),
                    VectorId(3),
                    VectorId(4),
                    VectorId(5),
                    VectorId(6),
                    VectorId(7),
                    VectorId(8)
                ]
                .as_ref()
            )
        );
        assert_eq!(
            hnsw.get_layer(0).map(|layer| &layer.neighbors),
            Some(
                vec![
                    NodeId(3),
                    NodeId(4),
                    NodeId(1),
                    NodeId(2),
                    NodeId(6),
                    NodeId(7),
                    NodeId(3),
                    NodeId(8),
                    NodeId(4),
                    NodeId(0),
                    NodeId(2),
                    NodeId(5),
                    NodeId(8),
                    NodeId(4),
                    NodeId(0),
                    NodeId(1),
                    NodeId(3),
                    NodeId(5),
                    NodeId(4),
                    NodeId(0),
                    NodeId(1),
                    NodeId(8),
                    NodeId(2),
                    NodeId(7),
                    NodeId(3),
                    NodeId(8),
                    NodeId(0),
                    NodeId(1),
                    NodeId(2),
                    NodeId(5),
                    NodeId(1),
                    NodeId(2),
                    NodeId(6),
                    NodeId(8),
                    NodeId(4),
                    NodeId(3),
                    NodeId(0),
                    NodeId(2),
                    NodeId(5),
                    NodeId(7),
                    NodeId(4),
                    NodeId(3),
                    NodeId(0),
                    NodeId(1),
                    NodeId(3),
                    NodeId(6),
                    NodeId(4),
                    NodeId(8),
                    NodeId(4),
                    NodeId(1),
                    NodeId(2),
                    NodeId(3),
                    NodeId(0),
                    NodeId(5)
                ]
                .as_ref()
            )
        );
    }

    #[test]
    fn test_search() {
        let hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        let data = &hnsw.layers[0].comparator.data;
        for (i, datum) in data.iter().enumerate() {
            let v = AbstractVector::Unstored(datum);
            let results = hnsw.search(v, hnsw.build_parameters.optimization.search);
            eprintln!("results: {results:?}");
            assert!(match_within_epsilon(VectorId(i), results));
        }
    }

    fn do_test_recall(hnsw: &Hnsw<BigComparator>, minimum_recall: f32) -> f32 {
        let data = &hnsw.layers[0].comparator.data;
        let total = data.len();
        let total_relevant: usize = data
            .par_iter()
            .enumerate()
            .map(|(i, datum)| {
                /* eprintln!("XXXXXXXXXXXXXXXXXXXXXX");
                eprintln!("Searching for {i}");
                */
                let v = AbstractVector::Unstored(datum);
                let results = hnsw.search(v, hnsw.build_parameters.optimization.search);
                if VectorId(i) == results[0].0 {
                    1
                } else {
                    0
                }
            })
            .sum();
        eprintln!("total relevant: {total_relevant}");
        eprintln!("from total: {total}");
        let recall = total_relevant as f32 / total as f32;
        eprintln!("with recall: {recall}");
        assert!(recall >= minimum_recall);

        recall
    }

    #[test]
    fn test_supers() {
        let size = 10000;
        let dimension = 10;
        let hnsw: Hnsw<BigComparator> = bigvec::make_random_hnsw(size, dimension);
        //eprintln!("Top neighbors: {:?}", hnsw.layers[0].neighbors);
        let supers_1 = hnsw.supers_for_layer(0);
        let supers_2 = hnsw.supers_for_layer(0);
        assert_eq!(supers_1, supers_2);
        let layer = hnsw.get_layer(0).unwrap();
        let node_distances1 = layer.node_distances(supers_1);
        let node_distances2 = layer.node_distances(supers_1);
        assert_eq!(node_distances1, node_distances2);
        let n1 = layer.discover_nodes_to_promote(supers_1);
        let n2 = layer.discover_nodes_to_promote(supers_1);
        assert_eq!(n1, n2);
        let v1 = hnsw.discover_unreachable_vectors(0, hnsw.build_parameters.optimization.search);
        eprintln!("{v1:?}");
        let v2 = hnsw.discover_unreachable_vectors(0, hnsw.build_parameters.optimization.search);
        assert_eq!(v1, v2);
        panic!();
    }

    #[test]
    fn test_recall() {
        let size = 10_000;
        let dimension = 1536;
        let bp = BuildParameters::default();
        let mut hnsw: Hnsw<BigComparator> =
            bigvec::make_random_hnsw_with_build_parameters(size, dimension, bp);
        do_test_recall(&hnsw, 0.9);
        eprintln!("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        eprintln!("Finished building, now improving");
        eprintln!("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        hnsw.improve_index(bp, None, &mut ());
        do_test_recall(&hnsw, 1.0);
        panic!();
    }

    #[test]
    fn test_promotion() {
        let size = 1000;
        let dimension = 50;
        let bp = BuildParameters::default();
        let mut hnsw: Hnsw<BigComparator> =
            bigvec::make_random_hnsw_with_build_parameters(size, dimension, bp);
        hnsw.improve_index(bp, None, &mut ());
        do_test_recall(&hnsw, 0.0);
        let mut improvement_count = 0;
        let mut last_recall = 0.0;
        let mut last_improvement = 1.0;
        while last_improvement > 0.001 {
            eprintln!("{improvement_count} time to improve index");
            hnsw.improve_index(bp, None, &mut ());
            let new_recall = do_test_recall(&hnsw, 0.0);
            last_improvement = new_recall - last_recall;
            last_recall = new_recall;
            eprintln!("improved index by {last_improvement}");
            improvement_count += 1;
            eprintln!("=========");
        }
        panic!();
    }

    #[test]
    fn test_layer_generation() {
        let size = 100_000;
        let dimension = 1536;
        let bp = BuildParameters::default();
        let mut hnsw: Hnsw<BigComparator> =
            bigvec::make_random_hnsw_with_build_parameters(size, dimension, bp);
        hnsw.improve_index(bp, None, &mut ());
        do_test_recall(&hnsw, 0.0);
        panic!()
    }

    #[test]
    fn test_small_index_improvement() {
        let mut hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        eprintln!("One from bottom: {:?}", hnsw.layers[hnsw.layer_count() - 2]);
        hnsw.improve_index(hnsw.build_parameters, None, &mut ());
        eprintln!(
            "One from bottom after: {:?}",
            hnsw.layers[hnsw.layer_count() - 2]
        );
        let data = &hnsw.layers[hnsw.layer_count() - 1].comparator.data;
        for (i, datum) in data.iter().enumerate() {
            let v = AbstractVector::Unstored(datum);
            let results = hnsw.search(v, hnsw.build_parameters.optimization.search);
            assert_eq!(VectorId(i), results[0].0)
        }
    }

    #[test]
    fn test_tiny_index_improvement() {
        let mut hnsw: Hnsw<SillyComparator> = make_broken_hnsw();
        hnsw.improve_index(hnsw.build_parameters, None, &mut ());
        let data = &hnsw.layers[hnsw.layer_count() - 1].comparator.data;
        for (i, datum) in data.iter().enumerate() {
            let v = AbstractVector::Unstored(datum);
            let results = hnsw.search(v, hnsw.build_parameters.optimization.search);
            eprintln!("{results:?}");
            assert_eq!(VectorId(i), results[0].0)
        }
    }

    #[test]
    fn test_partitions_with_single_entry() {
        let partitions = calculate_partitions(1, 24);
        assert_eq!(1, partitions.len());
    }

    #[test]
    fn test_neighborhood_order() {
        let size = 10_000;
        let dimension = 1536;
        let orders = vec![6, 12, 24];
        let mut best = 0.0_f32;
        let mut best_order = usize::MAX;
        let mut best_hnsw = None;
        let mut bp = BuildParameters::default();
        for order in orders {
            bp.order = order;
            let hnsw: Hnsw<BigComparator> =
                bigvec::make_random_hnsw_with_build_parameters(size, dimension, bp);
            let recall = do_test_recall(&hnsw, 0.0);
            if recall > best {
                best = recall;
                best_hnsw = Some(hnsw);
                best_order = order
            }
        }
        eprintln!("best_order: {best_order}");
        let mut improvement_count = 0;
        let mut last_recall = best;
        let mut last_improvement = 1.0;
        let mut hnsw = best_hnsw.unwrap();
        while last_improvement > 0.001 {
            eprintln!("{improvement_count} time to improve index");
            hnsw.improve_index(hnsw.build_parameters, None, &mut ());
            let new_recall = do_test_recall(&hnsw, 0.0);
            last_improvement = new_recall - last_recall;
            last_recall = new_recall;
            eprintln!("improved index by {last_improvement}");
            improvement_count += 1;
            eprintln!("=========");
        }

        panic!();
    }

    #[test]
    fn calculate_partitions_with_additions() {
        let order = 2;
        let mut sizes = calculate_partitions(1000, order);
        sizes.reverse();
        eprintln!("sizes: {:?}", sizes);
        let promotion_count = 100;
        let result = calculate_partitions_for_additions(&sizes[1..], promotion_count, order);
        let expected = vec![100, 50, 25, 13, 6, 3, 2, 1, 1, 1];
        assert_eq!(expected, result);
        panic!();
    }

    #[test]
    fn test_knn() {
        let hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        let sp = SearchParameters::default();
        let mut results: Vec<_> = hnsw.knn(1, sp).collect();
        results.sort_by_key(|(v, _d)| *v);
        assert_eq!(
            results,
            vec![
                (VectorId(0), vec![(VectorId(3), 0.29289323)]),
                (VectorId(1), vec![(VectorId(3), 0.29289323)]),
                (VectorId(2), vec![(VectorId(8), 0.29289323)]),
                (VectorId(3), vec![(VectorId(4), 0.1835745)]),
                (VectorId(4), vec![(VectorId(3), 0.1835745)]),
                (VectorId(5), vec![(VectorId(1), 1.0)]),
                (VectorId(6), vec![(VectorId(0), 1.0)]),
                (VectorId(7), vec![(VectorId(0), 1.0)]),
                (VectorId(8), vec![(VectorId(4), 0.1835745)])
            ]
        );
    }

    #[test]
    fn test_threshold_nn() {
        let hnsw: Hnsw<SillyComparator> = make_simple_hnsw();
        let params = SearchParameters::default();
        let mut results: Vec<_> = hnsw.threshold_nn(0.3, params).collect();
        results.sort_by_key(|(v, _d)| *v);
        assert_eq!(
            results,
            vec![
                (VectorId(0), vec![(VectorId(3), 0.29289323)]),
                (
                    VectorId(1),
                    vec![(VectorId(3), 0.29289323), (VectorId(8), 0.29289323)],
                ),
                (VectorId(2), vec![(VectorId(8), 0.29289323)]),
                (
                    VectorId(3),
                    vec![
                        (VectorId(4), 0.1835745),
                        (VectorId(0), 0.29289323),
                        (VectorId(1), 0.29289323),
                    ],
                ),
                (
                    VectorId(4),
                    vec![(VectorId(3), 0.1835745), (VectorId(8), 0.1835745)],
                ),
                (VectorId(5), vec![]),
                (VectorId(6), vec![]),
                (VectorId(7), vec![]),
                (
                    VectorId(8),
                    vec![
                        (VectorId(4), 0.1835745),
                        (VectorId(1), 0.29289323),
                        (VectorId(2), 0.29289323),
                    ],
                ),
            ]
        );
    }

    pub type Vec32 = Vec<f32>;
    #[derive(Clone, Debug, PartialEq)]
    pub struct Comparator32 {
        pub data: Arc<Vec<Vec32>>,
    }

    impl Comparator for Comparator32 {
        type T = BigVec;
        type Borrowable<'a> = &'a BigVec;
        fn compare_raw(&self, v1: &BigVec, v2: &BigVec) -> f32 {
            let mut result = 0.0;
            for (&f1, &f2) in v1.iter().zip(v2.iter()) {
                result += (f1 - f2).powi(2)
            }
            result.powf(0.5)
        }
        fn lookup(&self, v: VectorId) -> &BigVec {
            &self.data[v.0]
        }
    }

    pub fn random_vec(prng: &mut StdRng, size: usize) -> Vec<f32> {
        let range = Uniform::from(-1.0..1.0);
        let vec: Vec<f32> = prng.sample_iter(&range).take(size).collect();
        vec
    }

    #[test]
    fn test_euclidean() {
        let mut prng = StdRng::seed_from_u64(42);
        let size = 100_000;
        let vecs: Vec<Vec<f32>> = (0..size).map(move |_| random_vec(&mut prng, 32)).collect();
        let cc = Comparator32 { data: vecs.into() };
        let vids: Vec<VectorId> = (0..size).map(VectorId).collect();
        let mut bp = BuildParameters::default();
        bp.optimization.search.grid_network_dimension = 0;
        let mut hnsw: Hnsw<Comparator32> = Hnsw::generate(cc, vids, bp, &mut ());
        hnsw.improve_index(bp, None, &mut ());
        panic!()
    }

    #[test]
    fn test_calculate_partitions_for_additions() {
        let sizes = [1000];
        let number_of_promotions = 10;
        let order = 10;
        dbg!(calculate_partitions_for_additions(
            &sizes,
            number_of_promotions,
            order
        ));

        panic!();
    }

    #[test]
    fn test_final_idx() {
        let n = NodeId(0);
        let neighborhood_size = 10;
        let neighbors = [NodeId(!0); 10];
        let final_idx = get_final_neighbor_idx(neighborhood_size, &neighbors, n);
        assert_eq!(final_idx, 0);
        assert_eq!(neighbors[n.0..final_idx].to_vec(), vec![]);

        let n = NodeId(0);
        let neighborhood_size = 10;
        let neighbors = [NodeId(1); 10];
        let final_idx = get_final_neighbor_idx(neighborhood_size, &neighbors, n);
        assert_eq!(final_idx, 10);
        assert_eq!(neighbors[n.0..final_idx].to_vec(), vec![NodeId(1); 10]);

        let n = NodeId(0);
        let neighborhood_size = 10;
        let neighbors = vec![
            NodeId(1),
            NodeId(2),
            NodeId(3),
            NodeId(4),
            NodeId(5),
            NodeId(!0),
            NodeId(!0),
            NodeId(!0),
            NodeId(!0),
            NodeId(!0),
        ];
        let final_idx = get_final_neighbor_idx(neighborhood_size, &neighbors, n);
        assert_eq!(final_idx, 5);
        assert_eq!(
            neighbors[n.0..final_idx].to_vec(),
            vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
        );
    }
}
