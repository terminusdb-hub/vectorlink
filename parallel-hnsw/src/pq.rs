use std::{fs::File, path::PathBuf};

use chrono::Utc;
use linfa::traits::Fit;
use linfa::DatasetBase;
use linfa_clustering::KMeans;
use ndarray::{Array, Array2};
use rand::prelude::*;
use rayon::prelude::*;
use serde_json::json;

use crate::{
    keepalive,
    parameters::{BuildParameters, OptimizationParameters, PqBuildParameters, SearchParameters},
    progress::{PqProgressMonitor, ProgressMonitor, ProgressUpdate},
    utils::estimate_sample_size,
    AbstractVector, Comparator, Hnsw, OrderedFloat, Serializable, VectorId,
};

pub struct QuantizationStatistics {
    pub sample_avg: f32,
    pub sample_var: f32,
    pub sample_deviation: f32,
}

pub trait Quantizer<const SIZE: usize, const QUANTIZED_SIZE: usize> {
    fn quantize(&self, vec: &[f32; SIZE]) -> [u16; QUANTIZED_SIZE];
    fn reconstruct(&self, qvec: &[u16; QUANTIZED_SIZE]) -> [f32; SIZE];
}

pub trait PartialDistance {
    // A partial distance must be a sumable component
    fn partial_distance(&self, i: u16, j: u16) -> f32;
}

pub struct HnswQuantizer<
    const SIZE: usize,
    const CENTROID_SIZE: usize,
    const QUANTIZED_SIZE: usize,
    C,
> {
    hnsw: Hnsw<C>,
    pq_build_parameters: PqBuildParameters,
}

impl<const SIZE: usize, const CENTROID_SIZE: usize, const QUANTIZED_SIZE: usize, C>
    HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, C>
{
    pub fn new(hnsw: Hnsw<C>, pq_build_parameters: PqBuildParameters) -> Self {
        Self {
            hnsw,
            pq_build_parameters,
        }
    }

    pub fn comparator(&self) -> &C {
        self.hnsw.comparator()
    }
}

impl<
        const SIZE: usize,
        const CENTROID_SIZE: usize,
        const QUANTIZED_SIZE: usize,
        C: 'static + Comparator<T = [f32; CENTROID_SIZE]>,
    > Quantizer<SIZE, QUANTIZED_SIZE> for HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, C>
{
    fn quantize(&self, vec: &[f32; SIZE]) -> [u16; QUANTIZED_SIZE] {
        let sp = self.pq_build_parameters.quantized_search;
        let mut result = [0; QUANTIZED_SIZE];
        for (ix, v) in vec.chunks(CENTROID_SIZE).enumerate() {
            let v: &[f32; CENTROID_SIZE] = unsafe { &*(v.as_ptr() as *const [f32; CENTROID_SIZE]) };
            let distances = self.hnsw.search(AbstractVector::Unstored(v), sp);
            let quant = distances[0].0 .0 as u16; // TODO maybe debug assert
            result[ix] = quant;
        }
        result
    }

    fn reconstruct(&self, qvec: &[u16; QUANTIZED_SIZE]) -> [f32; SIZE] {
        let mut result = [0.0; SIZE];
        for (ix, i) in qvec.iter().enumerate() {
            let slice = &mut result[ix * CENTROID_SIZE..(ix + 1) * CENTROID_SIZE];
            let centroid = self.hnsw.comparator().lookup(VectorId(*i as usize));
            slice.copy_from_slice(&*centroid);
        }
        result
    }
}

impl<
        const SIZE: usize,
        const CENTROID_SIZE: usize,
        const QUANTIZED_SIZE: usize,
        ComparatorParams,
        C: 'static + Serializable<Params = ComparatorParams> + Clone + Sync,
    > Serializable for HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, C>
{
    type Params = ComparatorParams;

    fn serialize<P: AsRef<std::path::Path>>(
        &self,
        path: P,
    ) -> Result<(), crate::SerializationError> {
        let parameter_path = path.as_ref().join("pq_build_parameters.json");
        self.hnsw.serialize(path)?;
        let writer = File::create(parameter_path)?;
        serde_json::to_writer(writer, &self.pq_build_parameters)?;
        Ok(())
    }

    fn deserialize<P: AsRef<std::path::Path>>(
        path: P,
        params: Self::Params,
    ) -> Result<Self, crate::SerializationError> {
        let parameter_path = path.as_ref().join("pq_build_parameters.json");
        let hnsw = Hnsw::deserialize(path, params)?;
        let rdr = File::open(parameter_path)?;
        let pq_build_parameters = serde_json::from_reader(rdr)?;
        Ok(Self {
            hnsw,
            pq_build_parameters,
        })
    }
}

pub struct QuantizedHnsw<
    const SIZE: usize,
    const CENTROID_SIZE: usize,
    const QUANTIZED_SIZE: usize,
    CentroidComparator,
    QuantizedComparator,
    FullComparator,
> {
    quantizer: HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator>,
    hnsw: Hnsw<QuantizedComparator>,
    comparator: FullComparator,
}

pub trait VectorSelector {
    type T;
    fn selection_with_id(&self, size: usize) -> Vec<(VectorId, Self::T)>;
    fn selection(&self, size: usize) -> Vec<Self::T> {
        self.selection_with_id(size)
            .into_iter()
            .map(|(_, v)| v)
            .collect()
    }
    fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>>;
    fn num_vecs(&self) -> usize;
}

pub trait VectorStore {
    type T;
    fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId>;
    fn vecs(&self) -> impl Iterator<Item = &Self::T>;
}

pub trait CentroidComparatorConstructor: Comparator {
    fn new(centroids: Vec<Self::T>) -> Self;
}

pub trait QuantizedComparatorConstructor: Comparator {
    type CentroidComparator: Comparator;

    fn new(cc: &Self::CentroidComparator) -> Self;
}

impl<
        const SIZE: usize,
        const CENTROID_SIZE: usize,
        const QUANTIZED_SIZE: usize,
        CentroidComparator,
        QuantizedComparator,
        FullComparator,
    >
    QuantizedHnsw<
        SIZE,
        CENTROID_SIZE,
        QUANTIZED_SIZE,
        CentroidComparator,
        QuantizedComparator,
        FullComparator,
    >
{
    pub fn vector_count(&self) -> usize {
        self.hnsw.vector_count()
    }

    pub fn quantizer(
        &self,
    ) -> &HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator> {
        &self.quantizer
    }

    pub fn centroid_comparator(&self) -> &CentroidComparator {
        self.quantizer.comparator()
    }

    pub fn quantized_comparator(&self) -> &QuantizedComparator {
        self.hnsw.comparator()
    }

    pub fn full_comparator(&self) -> &FullComparator {
        &self.comparator
    }
}

impl<
        const SIZE: usize,
        const CENTROID_SIZE: usize,
        const QUANTIZED_SIZE: usize,
        CentroidComparator: Comparator<T = [f32; CENTROID_SIZE]> + CentroidComparatorConstructor + 'static,
        QuantizedComparator: Comparator<T = [u16; QUANTIZED_SIZE]>
            + VectorStore<T = [u16; QUANTIZED_SIZE]>
            + PartialDistance
            + QuantizedComparatorConstructor<CentroidComparator = CentroidComparator>
            + 'static,
        FullComparator: Comparator<T = [f32; SIZE]> + VectorSelector<T = [f32; SIZE]> + 'static,
    >
    QuantizedHnsw<
        SIZE,
        CENTROID_SIZE,
        QUANTIZED_SIZE,
        CentroidComparator,
        QuantizedComparator,
        FullComparator,
    >
{
    #[allow(unused)]
    fn kmeans_centroids(
        number_of_centroids: usize,
        subvector_selection_size: usize,
        comparator: &FullComparator,
    ) -> Vec<[f32; CENTROID_SIZE]> {
        let vector_selection = Self::random_centroids(subvector_selection_size, comparator);

        // Linfa
        let data: Vec<f32> = vector_selection
            .into_iter()
            .flat_map(|v| v.into_iter())
            .collect();
        let sub_length = data.len() / CENTROID_SIZE;
        let sub_arrays = Array::from_shape_vec((sub_length, CENTROID_SIZE), data).unwrap();
        eprintln!("sub_arrays: {sub_arrays:?}");
        let observations = DatasetBase::from(sub_arrays);
        // TODO review this number
        let number_of_clusters = usize::min(sub_length, number_of_centroids);
        let prng = StdRng::seed_from_u64(42);

        eprintln!("{} Running kmeans", Utc::now());
        let model = KMeans::params_with_rng(number_of_clusters, prng.clone())
            .tolerance(1e-2)
            .n_runs(1)
            .max_n_iterations(5)
            .fit(&observations)
            .expect("KMeans fitted");
        eprintln!("{} kmeans finished", Utc::now());
        let centroid_array: Array2<f32> = model.centroids().clone();
        centroid_array.len();
        let centroid_flat: Vec<f32> = centroid_array
            .into_shape(number_of_clusters * CENTROID_SIZE)
            .unwrap()
            .to_vec();
        eprintln!("centroid flat len: {}", centroid_flat.len());
        centroid_flat
            .chunks(CENTROID_SIZE)
            .map(|v| {
                let mut array = [0.0; CENTROID_SIZE];
                array.copy_from_slice(v);
                array
            })
            .collect()
    }

    fn random_centroids(
        number_of_centroids: usize,
        comparator: &FullComparator,
    ) -> Vec<[f32; CENTROID_SIZE]> {
        let selection = comparator.selection(number_of_centroids);
        let mut centroid_candidates: Vec<_> = selection
            .into_iter()
            .flat_map(|v| {
                (0..QUANTIZED_SIZE).map(move |quantum| {
                    let offset = quantum * CENTROID_SIZE;
                    let mut arr: [f32; CENTROID_SIZE] = [0.0; CENTROID_SIZE];
                    arr.copy_from_slice(&v[offset..(offset + CENTROID_SIZE)]);

                    arr
                })
            })
            .collect();
        centroid_candidates.sort_by_key(|c| c.iter().map(|x| OrderedFloat(*x)).collect::<Vec<_>>());
        centroid_candidates.dedup();
        let mut rng = thread_rng();
        centroid_candidates.shuffle(&mut rng);
        centroid_candidates.truncate(number_of_centroids);

        centroid_candidates
    }

    pub fn perform_quantization(
        comparator: FullComparator,
        centroid_quantizer: HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator>,
        mut quantized_comparator: QuantizedComparator,
        progress: &mut dyn ProgressMonitor,
    ) -> (
        Vec<VectorId>,
        HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator>,
        QuantizedComparator,
    ) {
        let mut vids: Vec<VectorId> = Vec::new();
        let mut iter = comparator.vector_chunks();
        eprintln!("quantizing");
        loop {
            let chunk = keepalive!(progress, iter.next());
            if chunk.is_none() {
                break;
            }
            let chunk = chunk.unwrap();
            timeit!({
                let quantized: Vec<_> = keepalive!(
                    progress,
                    chunk
                        .into_par_iter()
                        .map(|v| centroid_quantizer.quantize(&v))
                        .collect()
                );

                keepalive!(
                    progress,
                    vids.extend(quantized_comparator.store(Box::new(quantized.into_iter())))
                );
            })
        }
        std::mem::drop(iter);

        (vids, centroid_quantizer, quantized_comparator)
    }

    pub fn new_with_quantized_vectors(
        comparator: FullComparator,
        bp: PqBuildParameters,
        vids: Vec<VectorId>,
        centroid_quantizer: HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator>,
        quantized_comparator: QuantizedComparator,
        progress: &mut dyn ProgressMonitor,
    ) -> Self {
        eprintln!("generating");
        let hnsw: Hnsw<QuantizedComparator> =
            Hnsw::generate(quantized_comparator, vids, bp.hnsw, progress);
        Self {
            quantizer: centroid_quantizer,
            hnsw,
            comparator,
        }
    }

    pub fn generate_centroid_hnsw(
        comparator: FullComparator,
        number_of_centroids: usize,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> (Hnsw<CentroidComparator>, QuantizedComparator) {
        let centroids = keepalive!(
            progress,
            Self::random_centroids(number_of_centroids, &comparator)
        );
        eprintln!("Number of centroids: {}", centroids.len());

        let vector_ids = (0..centroids.len()).map(VectorId).collect();
        let centroid_comparator = keepalive!(progress, CentroidComparator::new(centroids));
        let quantized_comparator = QuantizedComparator::new(&centroid_comparator);
        let mut centroid_hnsw: Hnsw<CentroidComparator> = Hnsw::generate(
            centroid_comparator,
            vector_ids,
            bp,
            &mut PqProgressMonitor::wrap(progress),
        );
        centroid_hnsw.improve_index(bp, progress);
        (centroid_hnsw, quantized_comparator)
    }

    pub fn new(
        number_of_centroids: usize,
        comparator: FullComparator,
        bp: PqBuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> Self {
        //let centroids =
        //    Self::kmeans_centroids(number_of_centroids, 1 * number_of_centroids, &comparator);
        progress
            .update(ProgressUpdate {
                state: json!({"type":"pq"}),
            })
            .unwrap();

        let (centroid_hnsw, quantized_comparator) = Self::generate_centroid_hnsw(
            comparator.clone(),
            number_of_centroids,
            bp.centroids,
            progress,
        );

        let centroid_quantizer: HnswQuantizer<
            SIZE,
            CENTROID_SIZE,
            QUANTIZED_SIZE,
            CentroidComparator,
        > = HnswQuantizer {
            hnsw: centroid_hnsw,
            pq_build_parameters: bp,
        };

        let (vids, centroid_quantizer, quantized_comparator) = Self::perform_quantization(
            comparator.clone(),
            centroid_quantizer,
            quantized_comparator,
            progress,
        );

        Self::new_with_quantized_vectors(
            comparator,
            bp,
            vids,
            centroid_quantizer,
            quantized_comparator,
            progress,
        )
    }

    fn pq_to_natural_distance_queue(
        &self,
        v: AbstractVector<[f32; SIZE]>,
        distances: Vec<(VectorId, f32)>,
    ) -> Vec<(VectorId, f32)> {
        let mut reordered = Vec::with_capacity(distances.len());
        for (id, _) in distances {
            let dist = self
                .full_comparator()
                .compare_vec(AbstractVector::Stored(id), v.clone());
            reordered.push((id, dist))
        }
        reordered.sort_by_key(|(vid, d)| (OrderedFloat(*d), *vid));
        // TODO reorder
        reordered
    }

    pub fn search(
        &self,
        v: AbstractVector<[f32; SIZE]>,
        sp: SearchParameters,
    ) -> Vec<(VectorId, f32)> {
        let raw_v = self.comparator.lookup_abstract(v.clone());
        let quantized = self.quantizer.quantize(&raw_v);
        let result = self.hnsw.search(AbstractVector::Unstored(&quantized), sp);
        self.pq_to_natural_distance_queue(v, result)
    }

    pub fn improve_index(
        &mut self,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> f32 {
        self.hnsw.improve_index(bp, progress)
    }

    pub fn improve_index_at(
        &mut self,
        layer: usize,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> (f32, usize) {
        self.hnsw.improve_index_at(layer, bp, progress)
    }

    pub fn improve_neighbors(
        &mut self,
        op: OptimizationParameters,
        last_recall: Option<f32>,
    ) -> f32 {
        self.hnsw.improve_neighbors(op, last_recall)
    }

    pub fn promote_at_layer(
        &mut self,
        layer_from_top: usize,
        bp: BuildParameters,
        progress: &mut dyn ProgressMonitor,
    ) -> bool {
        self.hnsw.promote_at_layer(layer_from_top, bp, progress)
    }

    pub fn zero_neighborhood_size(&self) -> usize {
        self.hnsw.zero_neighborhood_size()
    }

    pub fn quantization_statistics(&self) -> QuantizationStatistics {
        let c = self.quantized_comparator();
        let quantized_vecs: Vec<_> = c.vecs().collect();
        let quantizer = self.quantizer();
        // sample_avg = sum(errors)/|errors|
        // sample_var = sum((error - sample_avg)^2)/|errors|

        let fc = self.full_comparator();
        let sample_size = estimate_sample_size(0.95, fc.num_vecs());
        eprintln!("sample size: {sample_size}");
        let reconstruction_error = vec![0.0_f32; sample_size];
        eprintln!("reconstruction_error: {reconstruction_error:?}");
        eprintln!("starting processing of vector chunks");
        fc.selection_with_id(sample_size)
            .into_par_iter()
            .map(|(vecid, full_vec)| (full_vec, &quantized_vecs[vecid.0]))
            .map(|(full_vec, quantized_vec)| {
                let reconstructed = quantizer.reconstruct(quantized_vec);

                fc.compare_raw(&full_vec, &reconstructed)
            })
            .enumerate()
            .for_each(|(ix, distance)| unsafe {
                eprintln!("at ix: {ix}");
                let ptr = reconstruction_error.as_ptr().add(ix) as *mut f32;
                *ptr = distance;
            });

        let sample_avg: f32 =
            reconstruction_error.iter().sum::<f32>() / reconstruction_error.len() as f32;
        let sample_var = reconstruction_error
            .iter()
            .map(|e| (e - sample_avg))
            .map(|x| x * x)
            .sum::<f32>()
            / (reconstruction_error.len() - 1) as f32;
        let sample_deviation = sample_var.sqrt();

        QuantizationStatistics {
            sample_avg,
            sample_var,
            sample_deviation,
        }
    }

    pub fn threshold_nn(
        &self,
        threshold: f32,
        search_parameters: SearchParameters,
    ) -> impl IndexedParallelIterator<Item = (VectorId, Vec<(VectorId, f32)>)> + '_ {
        let QuantizationStatistics {
            sample_avg,
            sample_deviation,
            ..
        } = self.quantization_statistics();
        eprintln!("threshold: {threshold}");
        let threshold = threshold + sample_avg + 2.0 * sample_deviation;
        eprintln!("sample_avg: {sample_avg}");
        eprintln!("sample_deviation: {sample_deviation}");
        eprintln!("recalculated threshold: {threshold}");

        self.hnsw
            .threshold_nn(threshold, search_parameters)
            .map(|(vid, queue)| {
                (
                    vid,
                    self.pq_to_natural_distance_queue(AbstractVector::Stored(vid), queue),
                )
            })
    }

    pub fn stochastic_recall(&self, optimization_parameters: OptimizationParameters) -> f32 {
        self.hnsw.stochastic_recall(optimization_parameters)
    }

    pub fn build_parameters_for_improve_index(&self) -> BuildParameters {
        self.hnsw.build_parameters
    }

    pub fn stochastic_reconstruction_cost(&self, proportion: f32) -> (f32, f32) {
        let total = self.hnsw.vector_count();
        let selection: usize = (total as f32 * proportion).ceil() as usize;
        let fc = self.full_comparator();
        let qc = self.quantized_comparator();
        let mut costs: Vec<f32> = Vec::with_capacity(selection);
        for i in 0..selection {
            let v = VectorId(i);
            let vector = fc.lookup(v);
            let quantized = qc.lookup(v);
            let reconstructed = self.quantizer.reconstruct(&quantized);
            let mut cost = 0.0;
            for i in 0..vector.len() {
                cost += (vector[i] - reconstructed[i]).powi(2);
            }
            costs.push(cost.sqrt())
        }
        let sum_cost: f32 = costs.iter().sum();
        let avg_cost = sum_cost / (selection as f32);
        let sigma = costs
            .iter()
            .map(|c| (c - avg_cost).powi(2))
            .sum::<f32>()
            .sqrt();
        (avg_cost, sigma)
    }
}

impl<
        const SIZE: usize,
        const CENTROID_SIZE: usize,
        const QUANTIZED_SIZE: usize,
        ComparatorParams,
        CentroidComparator: Serializable<Params = ()> + Clone + Sync + 'static,
        QuantizedComparator: Serializable<Params = CentroidComparator> + Clone + 'static,
        FullComparator: Serializable<Params = ComparatorParams> + 'static,
    > Serializable
    for QuantizedHnsw<
        SIZE,
        CENTROID_SIZE,
        QUANTIZED_SIZE,
        CentroidComparator,
        QuantizedComparator,
        FullComparator,
    >
{
    type Params = ComparatorParams;

    fn serialize<P: AsRef<std::path::Path>>(
        &self,
        path: P,
    ) -> Result<(), crate::SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();

        let quantizer_path = path_buf.join("quantizer");
        eprintln!("serializing quantizer");
        self.quantizer.serialize(quantizer_path)?;

        let hnsw_path = path_buf.join("hnsw");
        eprintln!("serializing hnsw");
        self.hnsw.serialize(hnsw_path)?;

        let comparator_path = path_buf.join("comparator");
        eprintln!("serializing comparator");
        self.comparator.serialize(comparator_path)?;

        Ok(())
    }

    fn deserialize<P: AsRef<std::path::Path>>(
        path: P,
        params: Self::Params,
    ) -> Result<Self, crate::SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();

        eprintln!("deserializing quantizer");
        let quantizer_path = path_buf.join("quantizer");
        let quantizer: HnswQuantizer<SIZE, CENTROID_SIZE, QUANTIZED_SIZE, CentroidComparator> =
            HnswQuantizer::deserialize(quantizer_path, ())?;
        let centroid_comparator = quantizer.comparator().clone();

        eprintln!("deserializing centroid hnsw");
        let hnsw_path = path_buf.join("hnsw");
        let hnsw: Hnsw<QuantizedComparator> = Hnsw::deserialize(hnsw_path, centroid_comparator)?;

        eprintln!("deserializing comparator");
        let comparator_path = path_buf.join("comparator");
        let full_comparator = FullComparator::deserialize(comparator_path, params)?;

        Ok(Self {
            quantizer,
            hnsw,
            comparator: full_comparator,
        })
    }
}

#[cfg(test)]
mod tests {
    fn clamp_01(f: f32) -> f32 {
        f.clamp(0.0, 1.0)
    }

    fn normalize_cosine_distance(f: f32) -> f32 {
        clamp_01((f - 1.0) / -2.0)
    }

    // assumes normalized vectors
    fn cosine32(v1: &[f32; 32], v2: &[f32; 32]) -> f32 {
        normalize_cosine_distance(
            v1.iter()
                .zip(v2.iter())
                .map(|(f1, f2)| f1 * f2)
                .sum::<f32>(),
        )
    }

    fn euclidean16(v1: &[f32; 16], v2: &[f32; 16]) -> f32 {
        v1.iter()
            .zip(v2.iter())
            .map(|(f1, f2)| (f1 - f2).powi(2))
            .sum::<f32>()
            .powf(0.5)
    }

    fn euclidean8(v1: &[f32; 8], v2: &[f32; 8]) -> f32 {
        v1.iter()
            .zip(v2.iter())
            .map(|(f1, f2)| (f1 - f2).powi(2))
            .sum::<f32>()
            .powf(0.5)
    }

    fn cosine1536(v1: &[f32; 1536], v2: &[f32; 1536]) -> f32 {
        normalize_cosine_distance(
            v1.iter()
                .zip(v2.iter())
                .map(|(f1, f2)| f1 * f2)
                .sum::<f32>(),
        )
    }

    use std::{
        ops::Deref,
        sync::{Arc, RwLock, RwLockReadGuard},
    };

    use crate::{bigvec::random_normed_vec, progress::SimpleProgressMonitor};

    use super::*;

    struct ReadLockedVec<'a, T> {
        lock: RwLockReadGuard<'a, Vec<T>>,
        id: VectorId,
    }

    impl<'a, T> Deref for ReadLockedVec<'a, T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            &self.lock[self.id.0]
        }
    }

    #[derive(Clone)]
    struct CentroidComparator16 {
        data: Arc<Vec<[f32; 16]>>,
    }

    impl Comparator for CentroidComparator16 {
        type T = [f32; 16];
        type Borrowable<'a> = &'a Self::T;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            &self.data[v.0]
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            euclidean16(v1, v2)
        }
    }

    impl CentroidComparatorConstructor for CentroidComparator16 {
        fn new(centroids: Vec<Self::T>) -> Self {
            Self {
                data: Arc::new(centroids),
            }
        }
    }

    #[derive(Clone)]
    struct QuantizedComparator16 {
        cc: CentroidComparator16,
        data: Arc<RwLock<Vec<[u16; 96]>>>,
    }

    impl PartialDistance for QuantizedComparator16 {
        fn partial_distance(&self, _i: u16, _j: u16) -> f32 {
            todo!()
        }
    }

    impl Comparator for QuantizedComparator16 {
        type T = [u16; 96];
        type Borrowable<'a> = ReadLockedVec<'a, Self::T>;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            ReadLockedVec {
                lock: self.data.read().unwrap(),
                id: v,
            }
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            let v_reconstruct1: Vec<f32> = v1
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            let v_reconstruct2: Vec<f32> = v2
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            let mut ar1 = [0.0_f32; 1536];
            let mut ar2 = [0.0_f32; 1536];
            ar1.copy_from_slice(&v_reconstruct1);
            ar2.copy_from_slice(&v_reconstruct2);
            cosine1536(&ar1, &ar2)
        }
    }

    impl QuantizedComparatorConstructor for QuantizedComparator16 {
        type CentroidComparator = CentroidComparator16;

        fn new(cc: &Self::CentroidComparator) -> Self {
            Self {
                cc: cc.clone(),
                data: Default::default(),
            }
        }
    }

    impl VectorStore for QuantizedComparator16 {
        type T = <QuantizedComparator16 as Comparator>::T;

        fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
            let mut data = self.data.write().unwrap();
            let vid = data.len();
            let mut vectors: Vec<VectorId> = Vec::new();
            data.extend(i.enumerate().map(|(i, v)| {
                vectors.push(VectorId(vid + i));
                v
            }));
            vectors
        }
    }

    #[derive(Clone)]
    struct CentroidComparator8 {
        data: Arc<Vec<[f32; 8]>>,
    }

    impl Comparator for CentroidComparator8 {
        type T = [f32; 8];
        type Borrowable<'a> = &'a Self::T;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            &self.data[v.0]
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            euclidean8(v1, v2)
        }
    }

    impl CentroidComparatorConstructor for CentroidComparator8 {
        fn new(centroids: Vec<Self::T>) -> Self {
            Self {
                data: Arc::new(centroids),
            }
        }
    }

    #[derive(Clone)]
    struct QuantizedComparator8 {
        cc: CentroidComparator8,
        data: Arc<RwLock<Vec<[u16; 192]>>>,
    }

    impl PartialDistance for QuantizedComparator8 {
        fn partial_distance(&self, _i: u16, _j: u16) -> f32 {
            todo!()
        }
    }

    impl Comparator for QuantizedComparator8 {
        type T = [u16; 192];
        type Borrowable<'a> = ReadLockedVec<'a, Self::T>;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            ReadLockedVec {
                lock: self.data.read().unwrap(),
                id: v,
            }
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            let v_reconstruct1: Vec<f32> = v1
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            let v_reconstruct2: Vec<f32> = v2
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            let mut ar1 = [0.0_f32; 1536];
            let mut ar2 = [0.0_f32; 1536];
            ar1.copy_from_slice(&v_reconstruct1);
            ar2.copy_from_slice(&v_reconstruct2);
            cosine1536(&ar1, &ar2)
        }
    }

    impl QuantizedComparatorConstructor for QuantizedComparator8 {
        type CentroidComparator = CentroidComparator8;

        fn new(cc: &Self::CentroidComparator) -> Self {
            Self {
                cc: cc.clone(),
                data: Default::default(),
            }
        }
    }

    impl VectorStore for QuantizedComparator8 {
        type T = <QuantizedComparator8 as Comparator>::T;

        fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
            let mut data = self.data.write().unwrap();
            let vid = data.len();
            let mut vectors: Vec<VectorId> = Vec::new();
            data.extend(i.enumerate().map(|(i, v)| {
                vectors.push(VectorId(vid + i));
                v
            }));
            vectors
        }
    }

    #[derive(Clone)]
    struct AIComparator {
        data: Arc<RwLock<Vec<[f32; 1536]>>>,
    }

    impl Comparator for AIComparator {
        type T = [f32; 1536];
        type Borrowable<'a> = ReadLockedVec<'a, Self::T>;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            ReadLockedVec {
                lock: self.data.read().unwrap(),
                id: v,
            }
        }

        fn compare_raw(&self, _v1: &Self::T, _v2: &Self::T) -> f32 {
            todo!()
        }
    }

    impl VectorStore for AIComparator {
        type T = <AIComparator as Comparator>::T;

        fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
            let mut data = self.data.write().unwrap();
            let vid = data.len();
            let mut vectors: Vec<VectorId> = Vec::new();
            data.extend(i.enumerate().map(|(i, v)| {
                vectors.push(VectorId(vid + i));
                v
            }));
            vectors
        }
    }

    impl VectorSelector for AIComparator {
        type T = <AIComparator as Comparator>::T;

        fn selection(&self, size: usize) -> Vec<Self::T> {
            self.data
                .read()
                .unwrap()
                .iter()
                .cloned()
                .take(size)
                .collect()
        }

        fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>> {
            vec![self.data.read().unwrap().clone()].into_iter()
        }

        fn num_vecs(&self) -> usize {
            self.data.read().unwrap().len()
        }
    }

    #[derive(Clone)]
    struct Comparator16 {
        data: Arc<RwLock<Vec<[f32; 16]>>>,
    }

    impl Comparator for Comparator16 {
        type T = [f32; 16];
        type Borrowable<'a> = ReadLockedVec<'a, Self::T>;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            ReadLockedVec {
                lock: self.data.read().unwrap(),
                id: v,
            }
        }

        fn compare_raw(&self, _v1: &Self::T, _v2: &Self::T) -> f32 {
            todo!()
        }
    }

    impl VectorStore for Comparator16 {
        type T = <Comparator16 as Comparator>::T;

        fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
            let mut data = self.data.write().unwrap();
            let vid = data.len();
            let mut vectors: Vec<VectorId> = Vec::new();
            data.extend(i.enumerate().map(|(i, v)| {
                vectors.push(VectorId(vid + i));
                v
            }));
            vectors
        }
    }

    impl VectorSelector for Comparator16 {
        type T = <Comparator16 as Comparator>::T;

        fn selection(&self, size: usize) -> Vec<Self::T> {
            self.data
                .read()
                .unwrap()
                .iter()
                .cloned()
                .take(size)
                .collect()
        }

        fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>> {
            vec![self.data.read().unwrap().clone()].into_iter()
        }

        fn num_vecs(&self) -> usize {
            self.data.read().unwrap().len()
        }
    }

    #[derive(Clone)]
    struct QuantizedComparator4 {
        cc: CentroidComparator4,
        data: Arc<RwLock<Vec<[u16; 4]>>>,
    }

    impl QuantizedComparatorConstructor for QuantizedComparator4 {
        type CentroidComparator = CentroidComparator4;

        fn new(cc: &Self::CentroidComparator) -> Self {
            Self {
                cc: cc.clone(),
                data: Default::default(),
            }
        }
    }

    impl PartialDistance for QuantizedComparator4 {
        fn partial_distance(&self, _i: u16, _j: u16) -> f32 {
            todo!()
        }
    }

    impl Comparator for QuantizedComparator4 {
        type T = [u16; 4];
        type Borrowable<'a> = ReadLockedVec<'a, Self::T>;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            ReadLockedVec {
                lock: self.data.read().unwrap(),
                id: v,
            }
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            let v_reconstruct1: Vec<f32> = v1
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            let v_reconstruct2: Vec<f32> = v2
                .iter()
                .flat_map(|i| self.cc.lookup(VectorId(*i as usize)).iter().copied())
                .collect();
            v_reconstruct1
                .iter()
                .zip(v_reconstruct2.iter())
                .map(|(f1, f2)| (f1 - f2).powi(2))
                .sum::<f32>()
                .sqrt()
        }
    }

    impl VectorStore for QuantizedComparator4 {
        type T = <QuantizedComparator4 as Comparator>::T;

        fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
            let mut data = self.data.write().unwrap();
            let vid = data.len();
            let mut vectors: Vec<VectorId> = Vec::new();
            data.extend(i.enumerate().map(|(i, v)| {
                vectors.push(VectorId(vid + i));
                v
            }));
            vectors
        }
    }

    #[derive(Clone)]
    struct CentroidComparator4 {
        data: Arc<Vec<[f32; 4]>>,
    }

    impl CentroidComparatorConstructor for CentroidComparator4 {
        fn new(centroids: Vec<Self::T>) -> Self {
            Self {
                data: Arc::new(centroids),
            }
        }
    }

    impl Comparator for CentroidComparator4 {
        type T = [f32; 4];
        type Borrowable<'a> = &'a Self::T;
        fn lookup(&self, v: crate::VectorId) -> Self::Borrowable<'_> {
            &self.data[v.0]
        }

        fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
            v1.iter()
                .zip(v2.iter())
                .map(|(f1, f2)| (f1 - f2).powi(2))
                .sum::<f32>()
                .sqrt()
        }
    }

    #[test]
    fn test_arrays() {
        let a = Array2::from_shape_vec((2, 3), vec![0, 1, 2, 3, 4, 5]).unwrap();
        let b = a.t();
        let x = a.clone().into_raw_vec();
        eprintln!("x: {x:?}");
        let y = b.into_owned().into_raw_vec();
        eprintln!("y: {y:?}");
        panic!();
    }

    #[test]
    fn test_pq() {
        let count = 10;
        let vecs: Vec<[f32; 1536]> = (0..count)
            .into_par_iter()
            .map(move |i| {
                let mut prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let mut arr = [0.0_f32; 1536];
                let v = random_normed_vec(&mut prng, 1536);
                arr.copy_from_slice(&v);
                arr
            })
            .collect();
        let fc = AIComparator {
            data: Arc::new(RwLock::new(vecs.clone())),
        };
        let bp = PqBuildParameters::default();
        let hnsw: QuantizedHnsw<1536, 16, 96, CentroidComparator16, QuantizedComparator16, _> =
            QuantizedHnsw::new(100, fc, bp, &mut SimpleProgressMonitor::default());
        let v = AbstractVector::Unstored(&vecs[0]);
        let res = hnsw.search(v, bp.hnsw.optimization.search);
        eprintln!("res: {res:?}");
        panic!();
    }

    #[test]
    fn test_small_pq() {
        let count = 10000;
        let vecs: Vec<[f32; 16]> = (0..count)
            .into_par_iter()
            .map(move |i| {
                let mut prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let mut arr = [0.0_f32; 16];
                let v = random_normed_vec(&mut prng, 16);
                arr.copy_from_slice(&v);
                arr
            })
            .collect();
        let fc = Comparator16 {
            data: Arc::new(RwLock::new(vecs.clone())),
        };
        let bp = PqBuildParameters::default();
        let mut hnsw: QuantizedHnsw<16, 4, 4, CentroidComparator4, QuantizedComparator4, _> =
            QuantizedHnsw::new(100, fc, bp, &mut SimpleProgressMonitor::default());
        hnsw.improve_neighbors(bp.hnsw.optimization, None);

        // Test last vector individually
        let raw_vec = vecs.last().unwrap();
        eprintln!("raw_vec: {raw_vec:?}");
        let quant_vec = hnsw.quantizer.quantize(raw_vec);
        eprintln!("quant_vec: {quant_vec:?}");
        let recon_vec = hnsw.quantizer.reconstruct(&quant_vec);
        eprintln!("recon_vec: {recon_vec:?}");
        let lvid = VectorId(vecs.len() - 1);
        let internal_quantized = *hnsw.quantized_comparator().lookup(lvid);
        eprintln!("internal quant: {internal_quantized:?}");
        let av = AbstractVector::Unstored(raw_vec);
        let res = hnsw.search(av, bp.hnsw.optimization.search);
        eprintln!("Match results: {res:?}");

        let mut matches = 0;
        let mut match_sum = 0.0;
        for (i, v) in vecs.iter().enumerate() {
            let av = AbstractVector::Unstored(v);
            let res = hnsw.search(av, bp.hnsw.optimization.search);
            if let Some((matchvid, match_distance)) = res.first() {
                if i == matchvid.0 {
                    matches += 1;
                    match_sum += match_distance;
                } else {
                    eprintln!("Match result for {i:?} {res:?}");
                }
            }
        }
        let recall = matches as f32 / vecs.len() as f32;
        eprintln!("recall: {recall}");
        let match_avg = match_sum / vecs.len() as f32;
        eprintln!("average match distance: {match_avg}");
        panic!();
    }
    #[test]
    fn centroid_hnsw() {
        let count = 100_000;
        let vecs: Vec<[f32; 1536]> = (0..count)
            .into_par_iter()
            .map(move |i| {
                let mut prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let mut arr = [0.0_f32; 1536];
                let v = random_normed_vec(&mut prng, 1536);
                arr.copy_from_slice(&v);
                arr
            })
            .collect();
        let number_of_centroids = 65535;
        let fc = AIComparator {
            data: Arc::new(RwLock::new(vecs)),
        };
        let centroids = QuantizedHnsw::<
            1536,
            16,
            96,
            CentroidComparator16,
            QuantizedComparator16,
            AIComparator,
        >::random_centroids(number_of_centroids, &fc);
        eprintln!("Number of centroids: {}", centroids.len());
        let vector_ids = (0..centroids.len()).map(VectorId).collect();
        let centroid_comparator = CentroidComparator16::new(centroids);
        //let quantized_comparator = QuantizedComparator16::new(&centroid_comparator);
        let bp = PqBuildParameters::default();
        let mut centroid_hnsw: Hnsw<CentroidComparator16> = Hnsw::generate(
            centroid_comparator,
            vector_ids,
            bp.centroids,
            &mut SimpleProgressMonitor::default(),
        );
        let recall =
            centroid_hnsw.improve_index(bp.centroids, &mut SimpleProgressMonitor::default());
        assert!(recall > 0.99);
    }

    #[test]
    fn test_pq_recall() {
        let count = 1_000;
        let vecs: Vec<[f32; 1536]> = (0..count)
            .into_par_iter()
            .map(move |i| {
                let mut prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let mut arr = [0.0_f32; 1536];
                let v = random_normed_vec(&mut prng, 1536);
                arr.copy_from_slice(&v);
                arr
            })
            .collect();
        let centroids = 65535;
        let fc = AIComparator {
            data: Arc::new(RwLock::new(vecs)),
        };
        let bp = PqBuildParameters::default();

        let mut hnsw: QuantizedHnsw<1536, 16, 96, CentroidComparator16, QuantizedComparator16, _> =
            QuantizedHnsw::new(centroids, fc, bp, &mut SimpleProgressMonitor::default());
        let (avg, std) = hnsw.stochastic_reconstruction_cost(1.0);
        eprintln!("Average cost: {avg}");
        eprintln!("Standard deviation: {std}");
        let recall = hnsw.improve_neighbors(bp.hnsw.optimization, None);
        assert_eq!(recall, 1.0)
    }

    #[test]
    fn test_pq_8_recall() {
        let count = 1_000;
        let vecs: Vec<[f32; 1536]> = (0..count)
            .into_par_iter()
            .map(move |i| {
                let mut prng = StdRng::seed_from_u64(42_u64 + i as u64);
                let mut arr = [0.0_f32; 1536];
                let v = random_normed_vec(&mut prng, 1536);
                arr.copy_from_slice(&v);
                arr
            })
            .collect();
        let centroids = 65535;
        let fc = AIComparator {
            data: Arc::new(RwLock::new(vecs)),
        };
        let bp = PqBuildParameters::default();

        let mut hnsw: QuantizedHnsw<1536, 8, 192, CentroidComparator8, QuantizedComparator8, _> =
            QuantizedHnsw::new(centroids, fc, bp, &mut SimpleProgressMonitor::default());
        let (avg, std) = hnsw.stochastic_reconstruction_cost(1.0);
        eprintln!("Average cost: {avg}");
        eprintln!("Standard deviation: {std}");
        let recall = hnsw.improve_neighbors(bp.hnsw.optimization, None);
        assert_eq!(recall, 1.0)
    }
}
