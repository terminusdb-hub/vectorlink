use std::{
    fs::{File, OpenOptions},
    io::{self, Read},
    marker::PhantomData,
    mem::MaybeUninit,
    ops::Range,
    os::{fd::AsRawFd, unix::fs::FileExt},
    path::Path,
};

use super::range::*;

pub struct VectorLoader<'a> {
    file: &'a File,
    vector_size: usize,
    upper_bound: Option<usize>,
}

impl<'a> VectorLoader<'a> {
    pub fn new(file: &'a File, vector_size: usize, upper_bound: Option<usize>) -> Self {
        Self {
            file,
            vector_size,
            upper_bound,
        }
    }

    pub fn vector_size(&self) -> usize {
        self.vector_size
    }

    pub fn load_range(&self, range: Range<usize>) -> io::Result<LoadedVectorRange> {
        assert!(range.end <= self.upper_bound.unwrap_or(!0));
        let size = self.vector_size * range.len();
        let mut vecs: Vec<u8> = Vec::with_capacity(size);
        {
            let buf = vecs.spare_capacity_mut();
            let bytes_buf =
                unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, size) };
            self.file.read_exact_at(bytes_buf, range.start as u64)?;
        }

        unsafe {
            vecs.set_len(range.len());
        }
        Ok(LoadedVectorRange::new(range, vecs.into_boxed_slice()))
    }

    /// # Safety
    /// This will not check whether T is the vector_size associated with this loader.
    pub unsafe fn load_sized_range_unchecked<T: Copy>(
        &self,
        range: Range<usize>,
    ) -> io::Result<LoadedSizedVectorRange<T>> {
        let vector_size = std::mem::size_of::<T>();
        let size = vector_size * range.len();
        let mut vecs: Vec<T> = Vec::with_capacity(range.len());
        {
            let buf = vecs.spare_capacity_mut();
            let bytes_buf =
                unsafe { std::slice::from_raw_parts_mut(buf.as_ptr() as *mut u8, size) };
            self.file.read_exact_at(bytes_buf, range.start as u64)?;
        }

        unsafe {
            vecs.set_len(range.len());
        }
        Ok(LoadedSizedVectorRange::new(range, vecs.into_boxed_slice()))
    }

    pub fn load_vec(&self, index: usize) -> io::Result<Box<[u8]>> {
        assert!(index < self.upper_bound.unwrap_or(!0));
        let mut data: Vec<u8> = Vec::with_capacity(self.vector_size);
        {
            let buf = data.spare_capacity_mut();
            let bytes_buf: &mut [u8] = unsafe { std::mem::transmute(buf) };
            self.file
                .read_exact_at(bytes_buf, (index * self.vector_size) as u64)?;
        }
        unsafe {
            data.set_len(self.vector_size);
        }

        Ok(data.into_boxed_slice())
    }

    pub fn load_sized_vec<T: Copy>(&self, index: usize) -> io::Result<T> {
        assert_eq!(std::mem::size_of::<T>(), self.vector_size);
        unsafe { self.load_sized_vec_unchecked(index) }
    }

    /// # Safety
    /// This will not check whether T is the vector_size associated with this loader.
    pub unsafe fn load_sized_vec_unchecked<T: Copy>(&self, index: usize) -> io::Result<T> {
        let mut result: MaybeUninit<T> = MaybeUninit::uninit();
        unsafe {
            let buf = std::slice::from_raw_parts_mut(
                result.as_mut_ptr() as *mut u8,
                std::mem::size_of::<T>(),
            );
            self.file
                .read_exact_at(buf, (index * self.vector_size) as u64)?;

            Ok(result.assume_init())
        }
    }

    pub fn into_sized<T: Copy>(self) -> SizedVectorLoader<'a, T> {
        assert_eq!(std::mem::size_of::<T>(), self.vector_size);
        SizedVectorLoader {
            inner: self,
            _x: PhantomData,
        }
    }
}

pub struct SizedVectorLoader<'a, T: Copy> {
    inner: VectorLoader<'a>,
    _x: PhantomData<T>,
}

impl<'a, T: Copy> SizedVectorLoader<'a, T> {
    pub fn new(file: &'a File, upper_bound: Option<usize>) -> Self {
        Self {
            inner: VectorLoader::new(file, std::mem::size_of::<T>(), upper_bound),
            _x: PhantomData,
        }
    }

    pub fn vector_size(&self) -> usize {
        self.inner.vector_size()
    }

    pub fn load_range(&self, range: Range<usize>) -> io::Result<LoadedSizedVectorRange<T>> {
        unsafe { self.inner.load_sized_range_unchecked(range) }
    }

    pub fn load_vec(&self, index: usize) -> io::Result<T> {
        unsafe { self.inner.load_sized_vec_unchecked(index) }
    }

    pub fn into_unsized(self) -> VectorLoader<'a> {
        self.inner
    }
}

pub struct SequentialVectorLoader<T> {
    file: File,
    chunk_size: usize,
    _x: PhantomData<T>,
}

impl<T> SequentialVectorLoader<T> {
    pub fn new(file: File, chunk_size: usize) -> Self {
        Self {
            file,
            chunk_size,
            _x: PhantomData,
        }
    }

    pub fn open<P: AsRef<Path>>(path: P, chunk_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let fd = file.as_raw_fd();
        let ret = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL) };
        if ret == 0 {
            Ok(Self::new(file, chunk_size))
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "fadvice failed"))
        }
    }

    pub fn load_chunk(&mut self) -> io::Result<Option<Vec<T>>> {
        let mut data: Vec<T> = Vec::with_capacity(self.chunk_size);
        let mut bytes_read = 0;
        {
            let buf = data.spare_capacity_mut();
            let bytes_buf = unsafe {
                std::slice::from_raw_parts_mut(
                    buf.as_ptr() as *mut u8,
                    buf.len() * std::mem::size_of::<T>(),
                )
            };
            loop {
                let count = self.file.read(&mut bytes_buf[bytes_read..])?;
                bytes_read += count;
                if count == 0 || bytes_read == buf.len() {
                    // done reading!
                    break;
                }
            }
        }
        if bytes_read == 0 {
            Ok(None)
        } else {
            // make sure that we read a multiple of T
            assert!(bytes_read % std::mem::size_of::<T>() == 0);
            unsafe {
                data.set_len(bytes_read / std::mem::size_of::<T>());
            }

            Ok(Some(data))
        }
    }
}

impl<T> Iterator for SequentialVectorLoader<T> {
    type Item = io::Result<Vec<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        // The iterator is a simple transformation of load_chunk, switching the option and the result
        match self.load_chunk() {
            Ok(None) => None,
            Ok(Some(v)) => Some(Ok(v)),
            Err(e) => Some(Err(e)),
        }
    }
}
