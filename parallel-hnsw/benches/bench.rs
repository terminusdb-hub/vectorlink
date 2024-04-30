#![feature(test)]
extern crate test;

use std::sync::Arc;

use parallel_hnsw::{Comparator, Hnsw, VectorId};
use rand::{thread_rng, Rng};
use test::Bencher;
type SillyVec = [f32; 100];
#[derive(Clone)]
struct SillyComparator {
    data: Arc<Vec<SillyVec>>,
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

fn generate_random_vector() -> SillyVec {
    let mut rng = thread_rng();

    let mut result: SillyVec = [0.0; 100];
    (0..result.len()).for_each(|i| {
        result[i] = rng.gen();
    });

    result
}

fn create_test_data(length: usize) -> SillyComparator {
    let mut vec = Vec::new();
    for _ in 0..length {
        vec.push(generate_random_vector());
    }
    SillyComparator {
        data: Arc::new(vec),
    }
}
