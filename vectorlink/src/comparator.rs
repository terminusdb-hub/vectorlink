use half::bf16;
use parallel_hnsw::pq::{
    CentroidComparatorConstructor, PartialDistance, QuantizedComparatorConstructor,
};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::{path::Path, sync::Arc};

use parallel_hnsw::{pq, Comparator, Serializable, SerializationError, VectorId};

use crate::store::{ImmutableVectorFile, LoadedVectorRange, VectorFile};
use crate::vecmath::{
    self, EuclideanDistance16, EuclideanDistance32, EuclideanDistance4, EuclideanDistance8,
    Quantized16Embedding, Quantized32Embedding, Quantized4Embedding, Quantized8Embedding,
    CENTROID_16_LENGTH, CENTROID_32_LENGTH, CENTROID_4_LENGTH, CENTROID_8_LENGTH,
    QUANTIZED_16_EMBEDDING_LENGTH, QUANTIZED_32_EMBEDDING_LENGTH, QUANTIZED_4_EMBEDDING_LENGTH,
    QUANTIZED_8_EMBEDDING_LENGTH,
};
use crate::{
    vecmath::{normalized_cosine_distance, Embedding},
    vectors::VectorStore,
};

#[derive(Clone)]
pub struct DiskOpenAIComparator {
    domain: String,
    vectors: Arc<ImmutableVectorFile<Embedding>>,
}

impl DiskOpenAIComparator {
    pub fn new(domain: String, vectors: Arc<ImmutableVectorFile<Embedding>>) -> Self {
        Self { domain, vectors }
    }
}

impl Comparator for DiskOpenAIComparator {
    type T = Embedding;
    type Borrowable<'a> = Box<Embedding>
        where Self: 'a;
    fn lookup(&self, v: VectorId) -> Box<Embedding> {
        Box::new(self.vectors.vec(v.0).unwrap())
    }

    fn compare_raw(&self, v1: &Embedding, v2: &Embedding) -> f32 {
        normalized_cosine_distance(v1, v2)
    }
}

impl Serializable for DiskOpenAIComparator {
    type Params = Arc<VectorStore>;
    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let mut comparator_file: std::fs::File = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)?;
        eprintln!("opened comparator serialize file");
        // How do we get this value?
        let comparator = ComparatorMeta {
            domain_name: self.domain.clone(),
            size: self.vectors.num_vecs(),
        };
        let comparator_meta = serde_json::to_string(&comparator)?;
        eprintln!("serialized comparator");
        comparator_file.write_all(&comparator_meta.into_bytes())?;
        eprintln!("wrote comparator to file");
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        store: Arc<VectorStore>,
    ) -> Result<Self, SerializationError> {
        let mut comparator_file = OpenOptions::new().read(true).open(path)?;
        let mut contents = String::new();
        comparator_file.read_to_string(&mut contents)?;
        let ComparatorMeta { domain_name, .. } = serde_json::from_str(&contents)?;
        let domain = store.get_domain(&domain_name)?;
        Ok(DiskOpenAIComparator {
            domain: domain.name().to_owned(),
            vectors: Arc::new(domain.immutable_file()),
        })
    }
}

impl pq::VectorSelector for DiskOpenAIComparator {
    type T = Embedding;

    fn selection(&self, size: usize) -> Vec<Self::T> {
        // TODO do something else for sizes close to number of vecs
        if size >= self.vectors.num_vecs() {
            return self.vectors.all_vectors().unwrap().clone().into_vec();
        }
        let mut rng = thread_rng();
        let mut set = HashSet::new();
        let range = Uniform::from(0_usize..self.vectors.num_vecs());
        while set.len() != size {
            let candidate = rng.sample(&range);
            set.insert(candidate);
        }

        set.into_iter()
            .map(|index| self.vectors.vec(index).unwrap())
            .collect()
    }

    fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>> {
        self.vectors
            .vector_chunks(1_000_000)
            .unwrap()
            .map(|x| x.unwrap())
    }
}

#[derive(Clone)]
pub struct OpenAIComparator {
    domain_name: String,
    range: Arc<LoadedVectorRange<Embedding>>,
}

impl OpenAIComparator {
    pub fn new(domain_name: String, range: Arc<LoadedVectorRange<Embedding>>) -> Self {
        Self { domain_name, range }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ComparatorMeta {
    domain_name: String,
    size: usize,
}

impl Comparator for OpenAIComparator {
    type T = Embedding;
    type Borrowable<'a> = &'a Embedding
        where Self: 'a;
    fn lookup(&self, v: VectorId) -> &Embedding {
        self.range.vec(v.0)
    }

    fn compare_raw(&self, v1: &Embedding, v2: &Embedding) -> f32 {
        normalized_cosine_distance(v1, v2)
    }
}

impl Serializable for OpenAIComparator {
    type Params = Arc<VectorStore>;
    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let mut comparator_file: std::fs::File =
            OpenOptions::new().write(true).create(true).open(path)?;
        eprintln!("opened comparator serialize file");
        // How do we get this value?
        let comparator = ComparatorMeta {
            domain_name: self.domain_name.clone(),
            size: self.range.len(),
        };
        let comparator_meta = serde_json::to_string(&comparator)?;
        eprintln!("serialized comparator");
        comparator_file.write_all(&comparator_meta.into_bytes())?;
        eprintln!("wrote comparator to file");
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        store: Arc<VectorStore>,
    ) -> Result<Self, SerializationError> {
        let mut comparator_file = OpenOptions::new().read(true).open(path)?;
        let mut contents = String::new();
        comparator_file.read_to_string(&mut contents)?;
        let ComparatorMeta { domain_name, .. } = serde_json::from_str(&contents)?;
        let domain = store.get_domain(&domain_name)?;
        Ok(OpenAIComparator {
            domain_name,
            range: Arc::new(domain.all_vecs()?),
        })
    }
}

struct MemoizedPartialDistances {
    partial_distances: Vec<bf16>,
    size: usize,
}

pub trait DistanceCalculator {
    type T;
    fn partial_distance(&self, left: &Self::T, right: &Self::T) -> f32;
    fn finalize_partial_distance(&self, distance: f32) -> f32;
    fn aggregate_partial_distances(&self, distances: &[f32]) -> f32;

    fn distance(&self, left: &Self::T, right: &Self::T) -> f32 {
        self.finalize_partial_distance(self.partial_distance(left, right))
    }
}

// i < j, i != j
#[inline]
fn index_to_offset(n: usize, i: usize, j: usize) -> usize {
    let i_f64 = i as f64;
    let j_f64 = j as f64;
    let n_f64 = n as f64;
    let correction = (i_f64 + 2.0) * (i_f64 + 1.0) / 2.0;
    (i_f64 * n_f64 + j_f64 - correction) as usize
}

// offset = i*n - (i + 2) * (i + 1) / 2 + j
//
fn offset_to_index(n: usize, offset: usize) -> (usize, usize) {
    let d = (2 * n - 1).pow(2) - 8 * offset;
    let i2 = (2 * n - 1) as f64 - (d as f64).sqrt();
    let i = (i2 / 2.0) as usize;
    let triangle = (i * (n - 1)) - ((i + 1) * i) / 2;
    let j = offset + 1 - triangle;
    (i, j)
}

#[inline]
fn triangle_lookup_length(n: usize) -> usize {
    index_to_offset(n, n - 2, n - 1) + 1
}

#[cfg(test)]
mod offsettest {
    use super::*;
    #[test]
    fn test_triangle_offsets() {
        let n = 100;
        let mut expected_index = 0;
        for i in 0..n {
            for j in 0..n {
                if i < j {
                    let actual = index_to_offset(n, i, j);
                    assert_eq!(expected_index, actual);
                    expected_index += 1;
                }
            }
        }
        assert_eq!(expected_index, triangle_lookup_length(n));
    }

    #[test]
    fn roundtrip() {
        let n = 65535;
        for i in 0..triangle_lookup_length(n) {
            let (a, b) = offset_to_index(n, i);
            if a >= n {
                eprintln!("Yikes: a: {a}, b: {b}, n: {n}");
            }
            if b >= n {
                eprintln!("Yikes: a: {a}, b: {b}, n: {n}");
            }
            assert!(a < n);
            assert!(b < n);

            if a == 0 && b == 0 {
                panic!("Failure at {i}: a: {a}, b: {b}, n: {n}");
            }
            let i2 = index_to_offset(n, a, b);
            if i != i2 {
                panic!("Failure n: {n}, a: {a}, b: {b}, i: {i}, i2: {i2}");
            }
            assert_eq!(i, i2);
        }
    }

    #[test]
    fn roundtrip_backwards() {
        let n = 65535;
        for a in 0..n {
            for b in 0..n {
                if a >= b {
                    continue;
                }
                let i = index_to_offset(n, a, b);
                let (a2, b2) = offset_to_index(n, i);
                if a2 != a || b2 != b {
                    panic!("omfg: a: {a}, b: {b}, a2: {a2}, b2: {b2}, i: {i}")
                }
            }
        }
    }
}

impl MemoizedPartialDistances {
    fn new<T, P: DistanceCalculator<T = T>>(partial_distance_calculator: P, vectors: &[T]) -> Self {
        eprintln!("constructing memoized");
        let memoized_array_length = triangle_lookup_length(vectors.len());
        eprintln!(
            "for size {} we figured {memoized_array_length}",
            vectors.len()
        );
        let mut partial_distances: Vec<bf16> = Vec::with_capacity(memoized_array_length);
        unsafe {
            partial_distances.set_len(memoized_array_length);
        }
        let size = vectors.len();
        for c in 0..memoized_array_length {
            let (i, j) = offset_to_index(size, c);
            if i > 65535 || j > 65535 {
                panic!("oh no {i} {j}");
            }
            partial_distances[c] = bf16::from_f32(
                partial_distance_calculator.partial_distance(&vectors[i], &vectors[j]),
            );
        }

        Self {
            partial_distances,
            size,
        }
    }

    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        let offset = match i.cmp(&j) {
            std::cmp::Ordering::Equal => {
                // Early bail
                return 0.0;
            }
            std::cmp::Ordering::Less => index_to_offset(self.size, i as usize, j as usize),
            std::cmp::Ordering::Greater => index_to_offset(self.size, j as usize, i as usize),
        };
        let distance: bf16 = self.partial_distances[offset];
        distance.to_f32()
    }
}

pub struct ArrayCentroidComparator<const N: usize, C> {
    distances: Arc<MemoizedPartialDistances>,
    centroids: Arc<LoadedVectorRange<[f32; N]>>,
    calculator: PhantomData<C>,
}

impl<const N: usize, C> Clone for ArrayCentroidComparator<N, C> {
    fn clone(&self) -> Self {
        Self {
            distances: self.distances.clone(),
            centroids: self.centroids.clone(),
            calculator: PhantomData,
        }
    }
}
unsafe impl<const N: usize, C> Sync for ArrayCentroidComparator<N, C> {}

pub type Centroid4Comparator = ArrayCentroidComparator<CENTROID_4_LENGTH, EuclideanDistance4>;
pub type Centroid8Comparator = ArrayCentroidComparator<CENTROID_8_LENGTH, EuclideanDistance8>;
pub type Centroid16Comparator = ArrayCentroidComparator<CENTROID_16_LENGTH, EuclideanDistance16>;
pub type Centroid32Comparator = ArrayCentroidComparator<CENTROID_32_LENGTH, EuclideanDistance32>;

impl<const SIZE: usize, C: DistanceCalculator<T = [f32; SIZE]> + Default>
    CentroidComparatorConstructor for ArrayCentroidComparator<SIZE, C>
{
    fn new(centroids: Vec<Self::T>) -> Self {
        let len = centroids.len();
        Self {
            distances: Arc::new(MemoizedPartialDistances::new(C::default(), &centroids)),
            centroids: Arc::new(LoadedVectorRange::new(centroids, 0..len)),
            calculator: PhantomData,
        }
    }
}

impl<const SIZE: usize, C: DistanceCalculator<T = [f32; SIZE]> + Default> Comparator
    for ArrayCentroidComparator<SIZE, C>
{
    type T = [f32; SIZE];

    type Borrowable<'a> = &'a Self::T where C: 'a;

    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
        &self.centroids[v.0]
    }

    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
        let calculator = C::default();
        calculator.distance(v1, v2)
    }
}

impl<const N: usize, C> PartialDistance for ArrayCentroidComparator<N, C> {
    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        self.distances.partial_distance(i, j)
    }
}

impl<const N: usize, C: DistanceCalculator<T = [f32; N]> + Default> Serializable
    for ArrayCentroidComparator<N, C>
{
    type Params = ();

    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let mut vector_file: VectorFile<[f32; N]> = VectorFile::create(path, true)?;
        vector_file.append_vector_range(self.centroids.vecs())?;

        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        _params: Self::Params,
    ) -> Result<Self, SerializationError> {
        let vector_file: VectorFile<[f32; N]> = VectorFile::open(path, true)?;
        let centroids = Arc::new(vector_file.all_vectors()?);

        Ok(Self {
            distances: Arc::new(MemoizedPartialDistances::new(
                C::default(),
                centroids.vecs(),
            )),
            centroids,
            calculator: PhantomData,
        })
    }
}

pub trait QuantizedData {
    type Quantized: Copy;
    fn data(&self) -> &Arc<LoadedVectorRange<Self::Quantized>>;
}

#[derive(Clone)]
pub struct Quantized32Comparator {
    pub cc: Centroid32Comparator,
    pub data: Arc<LoadedVectorRange<Quantized32Embedding>>,
}

impl QuantizedComparatorConstructor for Quantized32Comparator {
    type CentroidComparator = Centroid32Comparator;

    fn new(cc: &Self::CentroidComparator) -> Self {
        Self {
            cc: cc.clone(),
            data: Default::default(),
        }
    }
}

impl QuantizedData for Quantized32Comparator {
    type Quantized = Quantized32Embedding;

    fn data(&self) -> &Arc<LoadedVectorRange<Self::Quantized>> {
        &self.data
    }
}

#[derive(Clone)]
pub struct Quantized16Comparator {
    pub cc: Centroid16Comparator,
    pub data: Arc<LoadedVectorRange<Quantized16Embedding>>,
}

impl QuantizedComparatorConstructor for Quantized16Comparator {
    type CentroidComparator = Centroid16Comparator;

    fn new(cc: &Self::CentroidComparator) -> Self {
        Self {
            cc: cc.clone(),
            data: Default::default(),
        }
    }
}

impl QuantizedData for Quantized16Comparator {
    type Quantized = Quantized16Embedding;

    fn data(&self) -> &Arc<LoadedVectorRange<Self::Quantized>> {
        &self.data
    }
}

#[derive(Clone)]
pub struct Quantized8Comparator {
    pub cc: Centroid8Comparator,
    pub data: Arc<LoadedVectorRange<Quantized8Embedding>>,
}

impl QuantizedComparatorConstructor for Quantized8Comparator {
    type CentroidComparator = Centroid8Comparator;

    fn new(cc: &Self::CentroidComparator) -> Self {
        Self {
            cc: cc.clone(),
            data: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct Quantized4Comparator {
    pub cc: Centroid4Comparator,
    pub data: Arc<LoadedVectorRange<Quantized4Embedding>>,
}

impl QuantizedComparatorConstructor for Quantized4Comparator {
    type CentroidComparator = Centroid4Comparator;

    fn new(cc: &Self::CentroidComparator) -> Self {
        Self {
            cc: cc.clone(),
            data: Default::default(),
        }
    }
}

impl QuantizedData for Quantized4Comparator {
    type Quantized = Quantized4Embedding;

    fn data(&self) -> &Arc<LoadedVectorRange<Self::Quantized>> {
        &self.data
    }
}

impl QuantizedData for Quantized8Comparator {
    type Quantized = Quantized8Embedding;

    fn data(&self) -> &Arc<LoadedVectorRange<Self::Quantized>> {
        &self.data
    }
}

impl PartialDistance for Quantized32Comparator {
    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        self.cc.partial_distance(i, j)
    }
}

impl PartialDistance for Quantized16Comparator {
    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        self.cc.partial_distance(i, j)
    }
}

impl PartialDistance for Quantized8Comparator {
    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        self.cc.partial_distance(i, j)
    }
}

impl PartialDistance for Quantized4Comparator {
    fn partial_distance(&self, i: u16, j: u16) -> f32 {
        self.cc.partial_distance(i, j)
    }
}

impl Comparator for Quantized32Comparator
where
    Quantized32Comparator: PartialDistance,
{
    type T = Quantized32Embedding;

    type Borrowable<'a> = &'a Quantized32Embedding;

    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
        &self.data[v.0]
    }

    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
        let mut partial_distances = [0.0_f32; QUANTIZED_32_EMBEDDING_LENGTH];
        for ix in 0..QUANTIZED_32_EMBEDDING_LENGTH {
            let partial_1 = v1[ix];
            let partial_2 = v2[ix];
            let partial_distance = self.cc.partial_distance(partial_1, partial_2);
            partial_distances[ix] = partial_distance;
        }

        vecmath::sum_48(&partial_distances).sqrt()
    }
}

impl Serializable for Quantized32Comparator {
    type Params = ();

    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        std::fs::create_dir_all(&path_buf)?;

        let index_path = path_buf.join("index");
        self.cc.serialize(index_path)?;

        let vector_path = path_buf.join("vectors");
        let mut vector_file = VectorFile::create(vector_path, true)?;
        vector_file.append_vector_range(self.data.vecs())?;
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        _params: Self::Params,
    ) -> Result<Self, SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        let index_path = path_buf.join("index");
        let cc = Centroid32Comparator::deserialize(index_path, ())?;

        let vector_path = path_buf.join("vectors");
        let vector_file = VectorFile::open(vector_path, true)?;
        let range = vector_file.all_vectors()?;

        let data = Arc::new(range);
        Ok(Self { cc, data })
    }
}

impl pq::VectorStore for Quantized32Comparator {
    type T = <Quantized32Comparator as Comparator>::T;

    fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
        // this is p retty stupid, but then, these comparators should not be storing in the first place
        let mut new_contents: Vec<Self::T> = Vec::with_capacity(self.data.len() + i.size_hint().0);
        new_contents.extend(self.data.vecs().iter());
        let vid = self.data.len();
        let mut vectors: Vec<VectorId> = Vec::new();
        new_contents.extend(i.enumerate().map(|(i, v)| {
            vectors.push(VectorId(vid + i));
            v
        }));
        let end = new_contents.len();

        let data = LoadedVectorRange::new(new_contents, 0..end);
        self.data = Arc::new(data);

        vectors
    }
}

impl pq::VectorSelector for OpenAIComparator {
    type T = Embedding;

    fn selection(&self, size: usize) -> Vec<Self::T> {
        // TODO do something else for sizes close to number of vecs
        let mut rng = thread_rng();
        let mut set = HashSet::new();
        let range = Uniform::from(0_usize..size);
        while set.len() != size {
            let candidate = rng.sample(&range);
            set.insert(candidate);
        }

        set.into_iter()
            .map(|index| *self.range.vec(index))
            .collect()
    }

    fn vector_chunks(&self) -> impl Iterator<Item = Vec<Self::T>> {
        // low quality make better
        self.range.vecs().chunks(1_000_000).map(|c| c.to_vec())
    }
}

impl Comparator for Quantized16Comparator
where
    Quantized16Comparator: PartialDistance,
{
    type T = Quantized16Embedding;

    type Borrowable<'a> = &'a Self::T;

    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
        self.data.vec(v.0)
    }

    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
        let mut partial_distances = [0.0_f32; QUANTIZED_16_EMBEDDING_LENGTH];
        for ix in 0..QUANTIZED_16_EMBEDDING_LENGTH {
            let partial_1 = v1[ix];
            let partial_2 = v2[ix];
            let partial_distance = self.cc.partial_distance(partial_1, partial_2);
            partial_distances[ix] = partial_distance;
        }

        vecmath::sum_96(&partial_distances).sqrt()
    }
}

impl Serializable for Quantized16Comparator {
    type Params = ();

    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        std::fs::create_dir_all(&path_buf)?;

        let index_path = path_buf.join("index");
        self.cc.serialize(index_path)?;

        let vector_path = path_buf.join("vectors");
        let mut vector_file = VectorFile::create(vector_path, true)?;
        vector_file.append_vector_range(self.data.vecs())?;
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        _params: Self::Params,
    ) -> Result<Self, SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        let index_path = path_buf.join("index");
        let cc = Centroid16Comparator::deserialize(index_path, ())?;

        let vector_path = path_buf.join("vectors");
        let vector_file = VectorFile::open(vector_path, true)?;
        let range = vector_file.all_vectors()?;

        let data = Arc::new(range);
        Ok(Self { cc, data })
    }
}

impl pq::VectorStore for Quantized16Comparator {
    type T = <Quantized16Comparator as Comparator>::T;

    fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
        // this is p retty stupid, but then, these comparators should not be storing in the first place
        let mut new_contents: Vec<Self::T> = Vec::with_capacity(self.data.len() + i.size_hint().0);
        new_contents.extend(self.data.vecs().iter());
        let vid = self.data.len();
        let mut vectors: Vec<VectorId> = Vec::new();
        new_contents.extend(i.enumerate().map(|(i, v)| {
            vectors.push(VectorId(vid + i));
            v
        }));

        let end = new_contents.len();

        let data = LoadedVectorRange::new(new_contents, 0..end);
        self.data = Arc::new(data);

        vectors
    }
}

impl Comparator for Quantized8Comparator
where
    Quantized8Comparator: PartialDistance,
{
    type T = Quantized8Embedding;

    type Borrowable<'a> = &'a Self::T;

    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
        self.data.vec(v.0)
    }

    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
        let mut partial_distances = [0.0_f32; QUANTIZED_8_EMBEDDING_LENGTH];
        for ix in 0..QUANTIZED_8_EMBEDDING_LENGTH {
            let partial_1 = v1[ix];
            let partial_2 = v2[ix];
            let partial_distance = self.cc.partial_distance(partial_1, partial_2);
            partial_distances[ix] = partial_distance;
        }

        vecmath::sum_192(&partial_distances).sqrt()
    }
}

impl Serializable for Quantized8Comparator {
    type Params = ();

    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        std::fs::create_dir_all(&path_buf)?;

        let index_path = path_buf.join("index");
        self.cc.serialize(index_path)?;

        let vector_path = path_buf.join("vectors");
        let mut vector_file = VectorFile::create(vector_path, true)?;
        vector_file.append_vector_range(self.data.vecs())?;
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        _params: Self::Params,
    ) -> Result<Self, SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        let index_path = path_buf.join("index");
        let cc = Centroid8Comparator::deserialize(index_path, ())?;

        let vector_path = path_buf.join("vectors");
        let vector_file = VectorFile::open(vector_path, true)?;
        let range = vector_file.all_vectors()?;

        let data = Arc::new(range);
        Ok(Self { cc, data })
    }
}

impl pq::VectorStore for Quantized8Comparator {
    type T = <Quantized8Comparator as Comparator>::T;

    fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
        // this is p retty stupid, but then, these comparators should not be storing in the first place
        let mut new_contents: Vec<Self::T> = Vec::with_capacity(self.data.len() + i.size_hint().0);
        new_contents.extend(self.data.vecs().iter());
        let vid = self.data.len();
        let mut vectors: Vec<VectorId> = Vec::new();
        new_contents.extend(i.enumerate().map(|(i, v)| {
            vectors.push(VectorId(vid + i));
            v
        }));

        let end = new_contents.len();

        let data = LoadedVectorRange::new(new_contents, 0..end);
        self.data = Arc::new(data);

        vectors
    }
}

impl Comparator for Quantized4Comparator
where
    Quantized4Comparator: PartialDistance,
{
    type T = Quantized4Embedding;

    type Borrowable<'a> = &'a Self::T;

    fn lookup(&self, v: VectorId) -> Self::Borrowable<'_> {
        self.data.vec(v.0)
    }

    fn compare_raw(&self, v1: &Self::T, v2: &Self::T) -> f32 {
        let mut partial_distances = [0.0_f32; QUANTIZED_4_EMBEDDING_LENGTH];
        for ix in 0..QUANTIZED_4_EMBEDDING_LENGTH {
            let partial_1 = v1[ix];
            let partial_2 = v2[ix];
            let partial_distance = self.cc.partial_distance(partial_1, partial_2);
            partial_distances[ix] = partial_distance;
        }

        vecmath::sum_384(&partial_distances).sqrt()
    }
}

impl Serializable for Quantized4Comparator {
    type Params = ();

    fn serialize<P: AsRef<Path>>(&self, path: P) -> Result<(), SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        std::fs::create_dir_all(&path_buf)?;

        let index_path = path_buf.join("index");
        self.cc.serialize(index_path)?;

        let vector_path = path_buf.join("vectors");
        let mut vector_file = VectorFile::create(vector_path, true)?;
        vector_file.append_vector_range(self.data.vecs())?;
        Ok(())
    }

    fn deserialize<P: AsRef<Path>>(
        path: P,
        _params: Self::Params,
    ) -> Result<Self, SerializationError> {
        let path_buf: PathBuf = path.as_ref().into();
        let index_path = path_buf.join("index");
        let cc = Centroid4Comparator::deserialize(index_path, ())?;

        let vector_path = path_buf.join("vectors");
        let vector_file = VectorFile::open(vector_path, true)?;
        let range = vector_file.all_vectors()?;

        let data = Arc::new(range);
        Ok(Self { cc, data })
    }
}

impl pq::VectorStore for Quantized4Comparator {
    type T = <Quantized4Comparator as Comparator>::T;

    fn store(&mut self, i: Box<dyn Iterator<Item = Self::T>>) -> Vec<VectorId> {
        // this is p retty stupid, but then, these comparators should not be storing in the first place
        let mut new_contents: Vec<Self::T> = Vec::with_capacity(self.data.len() + i.size_hint().0);
        new_contents.extend(self.data.vecs().iter());
        let vid = self.data.len();
        let mut vectors: Vec<VectorId> = Vec::new();
        new_contents.extend(i.enumerate().map(|(i, v)| {
            vectors.push(VectorId(vid + i));
            v
        }));

        let end = new_contents.len();

        let data = LoadedVectorRange::new(new_contents, 0..end);
        self.data = Arc::new(data);

        vectors
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use parallel_hnsw::pq::CentroidComparatorConstructor;
    use parallel_hnsw::AbstractVector;

    use crate::comparator::Centroid32Comparator;
    use crate::comparator::Comparator;
    use crate::comparator::MemoizedPartialDistances;
    #[test]
    fn centroid32test() {
        /*
        let vectors = (0..1000)
            .map(|_| {
                let range = Uniform::from(0.0..1.0);
                let v: Vec<f32> = prng.sample_iter(&range).take(CENTROID_32_LENGTH).collect();
                v
            })
            .collect();
         */
        let cc = Centroid32Comparator::new(Vec::new());
        let mut v1 = [0.0_f32; 32];
        v1[0] = 1.0;
        v1[1] = 1.0;
        let mut v2 = [0.0_f32; 32];
        v2[30] = 1.0;
        v2[31] = 1.0;
        let res = cc.compare_vec(AbstractVector::Unstored(&v1), AbstractVector::Unstored(&v2));
        assert_eq!(res, 2.0);
    }
}
