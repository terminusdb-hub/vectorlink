use std::{
    io,
    ops::{Deref, DerefMut, Range},
    path::Path,
    sync::RwLock,
};

use urlencoding::encode;
use vectorlink_store::{
    file::{ImmutableVectorFile, VectorFile},
    loader::SequentialVectorLoader,
    range::LoadedSizedVectorRange,
};

use crate::vecmath::EMBEDDING_BYTE_LENGTH_1024;

pub struct Domain {
    name: String,
    file: RwLock<VectorFile>,
}

impl Domain {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn num_vecs(&self) -> usize {
        self.file().num_vecs()
    }

    pub fn open<P: AsRef<Path>>(dir: P, name: &str) -> io::Result<Self> {
        let mut path = dir.as_ref().to_path_buf();
        let encoded_name = encode(name);
        path.push(format!("{encoded_name}.vecs"));
        // TODO: this place should read the embedding length from a configuration file
        let file = RwLock::new(VectorFile::open_create(
            &path,
            EMBEDDING_BYTE_LENGTH_1024,
            true,
        )?);

        Ok(Domain {
            name: name.to_string(),
            file,
        })
    }

    pub fn file(&self) -> impl Deref<Target = VectorFile> + '_ {
        self.file.read().unwrap()
    }

    fn file_mut(&self) -> impl DerefMut<Target = VectorFile> + '_ {
        self.file.write().unwrap()
    }

    pub fn immutable_file(&self) -> ImmutableVectorFile {
        self.file().as_immutable()
    }

    #[allow(unused)]
    fn add_vecs<'a, T, I: Iterator<Item = &'a T>>(&self, vecs: I) -> io::Result<(usize, usize)>
    where
        T: 'a + Copy,
    {
        let mut vector_file = self.file_mut();
        let old_len = vector_file.num_vecs();
        let count = vector_file.as_sized_mut().append_vectors(vecs)?;

        Ok((old_len, count))
    }

    pub fn concatenate_file<P: AsRef<Path>>(&self, path: P) -> io::Result<(usize, usize)> {
        let mut self_file = self.file_mut();
        let read_vector_file = VectorFile::open(path, self_file.vector_byte_size(), true, true)?;
        let old_size = self.num_vecs();
        Ok((old_size, self_file.append_vector_file(&read_vector_file)?))
    }

    pub fn load_vec<T: Copy>(&self, id: usize) -> io::Result<T> {
        self.file().as_sized().vec(id)
    }

    pub fn vec_range<T: Copy>(&self, range: Range<usize>) -> io::Result<LoadedSizedVectorRange<T>> {
        self.file().as_sized().vector_range(range)
    }

    pub fn all_vecs<T: Copy>(&self) -> io::Result<LoadedSizedVectorRange<T>> {
        self.file().as_sized().all_vectors()
    }

    pub fn vector_chunks<T: Copy>(
        &self,
        chunk_size: usize,
    ) -> io::Result<SequentialVectorLoader<T>> {
        self.file().vector_chunks(chunk_size)
    }
}
