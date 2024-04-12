use std::{
    fs::{File, OpenOptions},
    io,
    marker::PhantomData,
    ops::Range,
    os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

use crate::{
    loader::{SequentialVectorLoader, SizedVectorLoader, VectorLoader},
    range::{LoadedSizedVectorRange, LoadedVectorRange},
};

pub struct VectorFile {
    path: PathBuf,
    file: File,
    num_vecs: usize,
    vector_byte_size: usize,
}

impl VectorFile {
    pub fn new(path: PathBuf, file: File, num_vecs: usize, vector_byte_size: usize) -> Self {
        Self {
            path,
            file,
            num_vecs,
            vector_byte_size,
        }
    }

    #[allow(unused)]
    pub fn create_new<P: AsRef<Path>>(
        path: P,
        vector_byte_size: usize,
        os_cached: bool,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        if !os_cached {
            options.custom_flags(libc::O_DIRECT);
        }
        let file = options.open(&path)?;
        Ok(Self::new(path, file, 0, vector_byte_size))
    }
    pub fn create<P: AsRef<Path>>(
        path: P,
        vector_byte_size: usize,
        os_cached: bool,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(true);
        if !os_cached {
            options.custom_flags(libc::O_DIRECT);
        }
        let file = options.open(&path)?;
        Ok(Self::new(path, file, 0, vector_byte_size))
    }

    pub fn open<P: AsRef<Path>>(
        path: P,
        vector_byte_size: usize,
        os_cached: bool,
    ) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(false);
        if !os_cached {
            options.custom_flags(libc::O_DIRECT);
        }

        let file = options.open(&path)?;
        let byte_size = file.metadata()?.size() as usize;

        assert!(byte_size % vector_byte_size == 0);

        let num_vecs = byte_size / vector_byte_size;

        Ok(Self::new(path, file, num_vecs, vector_byte_size))
    }

    pub fn open_create<P: AsRef<Path>>(
        path: P,
        vector_byte_size: usize,
        os_cached: bool,
    ) -> io::Result<Self> {
        if path.as_ref().exists() {
            Self::open(path, vector_byte_size, os_cached)
        } else {
            Self::create(path, vector_byte_size, os_cached)
        }
    }

    pub fn as_sized<T: Copy>(&mut self) -> SizedVectorFile<'_, T> {
        assert_eq!(std::mem::size_of::<T>(), self.vector_byte_size);

        SizedVectorFile {
            inner: self,
            _x: PhantomData,
        }
    }

    pub fn num_vecs(&self) -> usize {
        self.num_vecs
    }

    pub fn vector_loader(&self) -> VectorLoader {
        VectorLoader::new(&self.file, self.vector_byte_size, Some(self.num_vecs))
    }

    pub fn vector_chunks<T: Copy>(
        &self,
        chunk_size: usize,
    ) -> io::Result<SequentialVectorLoader<T>> {
        SequentialVectorLoader::open(&self.path, chunk_size)
    }

    pub fn as_immutable(&self) -> ImmutableVectorFile {
        ImmutableVectorFile(Self {
            path: self.path.clone(),
            file: self
                .file
                .try_clone()
                .expect("could not clone file handle while creating immutable vector filehandle"),
            num_vecs: self.num_vecs,
            vector_byte_size: self.vector_byte_size,
        })
    }
}

pub struct SizedVectorFile<'a, T: Copy> {
    inner: &'a mut VectorFile,
    _x: PhantomData<T>,
}

impl<'a, T: Copy> SizedVectorFile<'a, T> {
    pub fn num_vecs(&self) -> usize {
        self.inner.num_vecs()
    }

    pub fn append_vector_range(&mut self, vectors: &[T]) -> io::Result<usize> {
        let vector_bytes = unsafe {
            std::slice::from_raw_parts(
                vectors.as_ptr() as *const u8,
                std::mem::size_of_val(vectors),
            )
        };
        self.inner.file.write_all_at(
            vector_bytes,
            (self.inner.num_vecs * std::mem::size_of::<T>()) as u64,
        )?;
        self.inner.num_vecs += vectors.len();
        self.inner.file.sync_data()?;

        Ok(vectors.len())
    }
    pub fn append_vectors<'b, I: Iterator<Item = &'b T>>(&mut self, vectors: I) -> io::Result<usize>
    where
        T: 'b,
    {
        // wouldn't it be more straightforward to just use the file as a cursor?
        let mut offset = (self.inner.num_vecs * std::mem::size_of::<T>()) as u64;
        let mut count = 0;
        for vector in vectors {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    vector as *const T as *const u8,
                    std::mem::size_of::<T>(),
                )
            };
            self.inner.file.write_all_at(bytes, offset)?;
            self.inner.num_vecs += 1;
            offset += std::mem::size_of::<T>() as u64;
            count += 1;
        }

        self.inner.file.sync_data()?;

        Ok(count)
    }

    pub fn append_vector_file(&mut self, file: &SizedVectorFile<T>) -> io::Result<usize> {
        let mut read_offset = 0;
        let mut write_offset = (self.inner.num_vecs * std::mem::size_of::<T>()) as u64;

        let num_vecs_to_write = file.inner.num_vecs;
        let mut num_bytes_to_write = num_vecs_to_write * std::mem::size_of::<T>();

        let mut buf = vec![0_u8; 4096];
        while num_bytes_to_write != 0 {
            let n = file.inner.file.read_at(&mut buf, read_offset)?;
            self.inner.file.write_all_at(&buf[..n], write_offset)?;
            num_bytes_to_write -= n;
            read_offset += n as u64;
            write_offset += n as u64;
        }
        self.inner.file.sync_data()?;

        Ok(num_vecs_to_write)
    }

    pub fn vector_loader(&self) -> SizedVectorLoader<T> {
        self.inner.vector_loader().into_sized()
    }

    pub fn vector_range(&self, range: Range<usize>) -> io::Result<LoadedSizedVectorRange<T>> {
        self.vector_loader().load_range(range)
    }

    pub fn vec(&self, index: usize) -> io::Result<T> {
        self.vector_loader().load_vec(index)
    }

    pub fn all_vectors(&self) -> io::Result<LoadedSizedVectorRange<T>> {
        self.vector_loader().load_range(0..self.inner.num_vecs)
    }
}

pub struct ImmutableVectorFile(VectorFile);
impl Clone for ImmutableVectorFile {
    fn clone(&self) -> Self {
        Self(VectorFile {
            path: self.0.path.clone(),
            file: self
                .0
                .file
                .try_clone()
                .expect("could not clone file handle while creating immutable vector filehandle"),
            num_vecs: self.0.num_vecs,
            vector_byte_size: self.0.vector_byte_size,
        })
    }
}

impl ImmutableVectorFile {
    pub fn vector_loader(&self) -> VectorLoader {
        VectorLoader::new(&self.0.file, self.0.vector_byte_size, Some(self.0.num_vecs))
    }

    pub fn vector_range(&self, range: Range<usize>) -> io::Result<LoadedVectorRange> {
        self.0.vector_loader().load_range(range)
    }

    pub fn vec(&self, index: usize) -> io::Result<Box<[u8]>> {
        self.0.vector_loader().load_vec(index)
    }

    pub fn all_vectors(&self) -> io::Result<LoadedVectorRange> {
        self.0.vector_loader().load_range(0..self.0.num_vecs)
    }

    pub fn num_vecs(&self) -> usize {
        self.0.num_vecs
    }

    pub fn vector_chunks<T: Copy>(
        &self,
        chunk_size: usize,
    ) -> io::Result<SequentialVectorLoader<T>> {
        self.0.vector_chunks(chunk_size)
    }
}
