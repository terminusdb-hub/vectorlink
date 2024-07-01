use statrs::distribution::{ContinuousCDF, Normal};

#[macro_export]
macro_rules! timeit {
    ($e:expr) => {{
        let e_str = stringify!($e);
        let start_time = chrono::Local::now();
        let result = $e;
        let end_time = chrono::Local::now();
        let elapsed_time = end_time - start_time;
        eprintln!("{elapsed_time}: {e_str}");

        result
    }};
}

pub fn estimate_sample_size(recall_confidence: f32, total: usize) -> usize {
    let normal = Normal::new(0.0, 1.0).unwrap();
    let z = normal.inverse_cdf(recall_confidence as f64);
    usize::min(
        usize::max(1, (z.powi(2) * (total as f64).powf(0.5)) as usize),
        total,
    )
}
