use std::{
    marker::PhantomData,
    ops::{Index, Range},
};

/// A range of vectors loaded into memory.
#[derive(Default)]
pub struct LoadedVectorRange {
    range: Range<usize>,
    vecs: Box<[u8]>,
}

/// A range of vectors of type T loaded into memory.
#[derive(Default)]
pub struct LoadedSizedVectorRange<T: Copy> {
    inner: LoadedVectorRange,
    _x: PhantomData<T>,
}

impl LoadedVectorRange {
    pub fn new(range: Range<usize>, vecs: Box<[u8]>) -> Self {
        assert_eq!(
            0,
            vecs.len() % range.len(),
            "given vecs data cannot be interpreted as range.len() vecs"
        );

        Self { range, vecs }
    }

    pub fn vector_size(&self) -> usize {
        debug_assert!(self.vecs.len() % self.range.len() == 0);
        self.vecs.len() / self.range.len()
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub fn into_sized<T: Copy>(self) -> LoadedSizedVectorRange<T> {
        debug_assert!(self.vecs.len() % self.range.len() == 0);
        assert_eq!(
            self.vector_size(),
            std::mem::size_of::<T>(),
            "tried to convert LoadedVectorRange to LoadedSizedVectorRange with wrong size"
        );

        unsafe { std::mem::transmute(self) }
    }
}

impl<T: Copy> LoadedSizedVectorRange<T> {
    pub fn new(range: Range<usize>, mut vecs: Box<[T]>) -> Self {
        assert_eq!(range.len(), vecs.len());
        let vecs_len = vecs.len() * std::mem::size_of::<T>();
        let vecs_ptr = vecs.as_mut_ptr() as *mut u8;
        unsafe {
            let slice = std::slice::from_raw_parts_mut(vecs_ptr, vecs_len);
            let converted_vecs = Box::from_raw(slice);
            std::mem::forget(vecs);
            Self {
                inner: LoadedVectorRange {
                    range,
                    vecs: converted_vecs,
                },
                _x: PhantomData,
            }
        }
    }
    pub fn vector_size(&self) -> usize {
        std::mem::size_of::<T>()
    }

    pub fn len(&self) -> usize {
        self.inner.range.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn into_unsized(self) -> LoadedVectorRange {
        self.inner
    }

    pub fn vecs(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.inner.vecs.as_ptr() as *const T, self.len()) }
    }
}

impl Index<usize> for LoadedVectorRange {
    type Output = [u8];

    fn index(&self, index: usize) -> &Self::Output {
        assert!(self.range.contains(&index));
        let corrected_index = index - self.range.start;
        let vector_size = self.vector_size();
        let offset = corrected_index * vector_size;
        &self.vecs[offset..offset + vector_size]
    }
}

impl<T: Copy> Index<usize> for LoadedSizedVectorRange<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(self.inner.range.contains(&index));
        unsafe {
            let vecs = self.inner.vecs.as_ptr() as *const T;
            &*vecs.add(index - self.inner.range.start)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sized_vector_range() {
        let elements = vec![[1, 2, 3], [4, 5, 6], [7, 8, 0]];
        let range =
            LoadedSizedVectorRange::new(0..elements.len(), elements.clone().into_boxed_slice());
        assert_eq!(range.len(), elements.len());
        for i in 0..elements.len() {
            assert_eq!(elements[i], range[i]);
        }

        assert_eq!(&elements, range.vecs());
    }

    #[test]
    #[should_panic]
    fn sized_vector_range_from_wrong_range() {
        let elements = vec![[1, 2, 3], [4, 5, 6], [7, 8, 0]];
        LoadedSizedVectorRange::new(0..2, elements.into_boxed_slice());
    }

    #[test]
    fn unsized_vector_range() {
        let elements = vec![1_u8, 2, 3, 4, 5, 6, 7, 8, 9];
        let range = LoadedVectorRange::new(0..3, elements.clone().into_boxed_slice());
        assert_eq!(range.len(), 3);
        for i in 0..range.len() {
            assert_eq!(elements[i * 3..(i + 1) * 3], range[i]);
        }
    }

    #[test]
    #[should_panic]
    fn unsized_vector_range_from_wrong_range() {
        let elements = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        LoadedVectorRange::new(0..2, elements.clone().into_boxed_slice());
    }

    #[test]
    fn unsized_vector_range_into_sized() {
        let elements = vec![1_u8, 2, 3, 4, 5, 6, 7, 8, 9];
        let range = LoadedVectorRange::new(0..3, elements.clone().into_boxed_slice());
        let sized = range.into_sized::<[u8; 3]>();
        assert_eq!([1, 2, 3], sized[0]);
        assert_eq!([4, 5, 6], sized[1]);
        assert_eq!([7, 8, 9], sized[2]);
    }

    #[test]
    fn sized_vector_range_into_unsized() {
        let vecs = vec![[1_u8, 2, 3], [4, 5, 6], [7, 8, 9]];

        let range = LoadedSizedVectorRange::new(0..3, vecs.clone().into_boxed_slice());
        let unsized_range = range.into_unsized();
        for i in 0..vecs.len() {
            assert_eq!(vecs[i], unsized_range[i]);
        }
    }
}
