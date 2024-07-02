use rayon::prelude::*;

use parallel_hnsw::{
    pq::{PartialDistance, QuantizedHnsw, Quantizer, VectorSelector, VectorStore},
    utils::estimate_sample_size,
    Comparator,
};

use crate::comparator::QuantizedData;

pub struct QuantizationStatistics {
    pub sample_avg: f32,
    pub sample_var: f32,
    pub sample_deviation: f32,
}

pub fn test_quantization<
    const SIZE: usize,
    const CENTROID_SIZE: usize,
    const QUANTIZED_SIZE: usize,
    CentroidComparator: 'static + Comparator<T = [f32; CENTROID_SIZE]>,
    QuantizedComparator: Comparator<T = [u16; QUANTIZED_SIZE]>
        + VectorStore<T = [u16; QUANTIZED_SIZE]>
        + PartialDistance
        + QuantizedData<Quantized = [u16; QUANTIZED_SIZE]>
        + 'static,
    FullComparator: Comparator<T = [f32; SIZE]> + VectorSelector<T = [f32; SIZE]> + 'static,
>(
    hnsw: &QuantizedHnsw<
        SIZE,
        CENTROID_SIZE,
        QUANTIZED_SIZE,
        CentroidComparator,
        QuantizedComparator,
        FullComparator,
    >,
) -> QuantizationStatistics {
    let c = hnsw.quantized_comparator();
    let quantized_vecs = c.data().vecs();
    let quantizer = hnsw.quantizer();
    // sample_avg = sum(errors)/|errors|
    // sample_var = sum((error - sample_avg)^2)/|errors|

    let fc = hnsw.full_comparator();
    let sample_size = estimate_sample_size(0.95, fc.num_vecs());
    let reconstruction_error = vec![0.0_f32; sample_size];
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
