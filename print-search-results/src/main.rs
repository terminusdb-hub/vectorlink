use byteorder::{NativeEndian, ReadBytesExt};
use clap::Parser;
use parallel_hnsw::VectorId;
use std::io::Read;
use std::{fs::File, io::Seek, io::SeekFrom};

#[derive(Parser, Debug)]
struct Command {
    #[arg(short, long)]
    vector: usize,
    #[arg(short, long)]
    prefix: String,
}

fn main() {
    let args = Command::parse();
    let mut index = File::open(format!("{}.index", args.prefix)).unwrap();
    let mut queues = File::open(format!("{}.queues", args.prefix)).unwrap();

    index
        .seek(SeekFrom::Start((args.vector * 8) as u64))
        .unwrap();
    let size = index.read_u64::<NativeEndian>().unwrap();
    index
        .seek(SeekFrom::Start(((args.vector + 1) * 8) as u64))
        .unwrap();
    let next_size = index.read_u64::<NativeEndian>().unwrap();

    let buf_size = (next_size - size) as usize;
    eprintln!("Expected buf size: {buf_size}");
    let mut buf: Vec<u8> = vec![0; buf_size];

    queues.seek(SeekFrom::Start(size)).unwrap();
    queues.read_exact(&mut buf).unwrap();
    let record_size = std::mem::size_of::<(VectorId, f32)>();

    assert_eq!(buf_size % record_size, 0);
    let queue: &[(VectorId, f32)] = unsafe {
        std::slice::from_raw_parts(
            buf.as_ptr() as *const (VectorId, f32),
            buf_size / record_size,
        )
    };

    println!("{queue:?}");
}
