use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::{mem, slice};
use thiserror::Error;

use crate::parameters::BuildParameters;
use crate::{Hnsw, Layer, NodeId, Serializable, VectorId};

#[derive(Error, Debug)]
pub enum SerializationError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error("Index not found")]
    IndexNotFound,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LayerMeta {
    pub node_count: usize,
    pub neighborhood_size: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HNSWMeta {
    pub layer_count: usize,
    pub build_parameters: BuildParameters,
}

pub fn serialize_hnsw<C: Serializable, P: AsRef<Path>>(
    build_parameters: BuildParameters,
    layers: &[Layer<C>],
    path: P,
) -> Result<(), SerializationError> {
    let layer_count = layers.len();

    std::fs::create_dir_all(&path)?;
    let mut hnsw_meta: PathBuf = path.as_ref().into();
    hnsw_meta.push("meta");
    eprintln!("hnsw serialization path: {hnsw_meta:?}");
    let mut hnsw_meta_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(hnsw_meta)?;
    eprintln!("opened hnsw file");

    let serialized = serde_json::to_string(&HNSWMeta {
        layer_count,
        build_parameters,
    })?;
    eprintln!("serialized data");
    hnsw_meta_file.write_all(serialized.as_bytes())?;
    eprintln!("serialized to file");

    if layer_count > 0 {
        let mut hnsw_comparator: PathBuf = path.as_ref().into();
        hnsw_comparator.push("comparator");
        layers[0].comparator.serialize(hnsw_comparator)?;
        eprintln!("serializing comparator");
    }

    for (i, layer) in layers.iter().enumerate().take(layer_count) {
        let layer_number = layer_count - i - 1;

        // Write meta data
        let mut hnsw_layer_meta: PathBuf = path.as_ref().into();
        hnsw_layer_meta.push(format!("layer.meta.{layer_number}"));
        let mut hnsw_layer_meta_file: std::fs::File = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&hnsw_layer_meta)?;
        eprintln!("opened {hnsw_layer_meta:?} for layer {layer_number}");
        let neighborhood_size = layer.neighborhood_size;
        let node_count = layer.nodes.len();
        let layer_meta = serde_json::to_string(&LayerMeta {
            node_count,
            neighborhood_size,
        })?;
        hnsw_layer_meta_file.write_all(&layer_meta.into_bytes())?;
        eprintln!("wrote meta for layer {layer_number}");

        // Write Nodes
        let mut hnsw_layer_nodes: PathBuf = path.as_ref().into();
        hnsw_layer_nodes.push(format!("layer.nodes.{layer_number}"));
        let mut hnsw_layer_nodes_file: std::fs::File = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&hnsw_layer_nodes)?;
        eprintln!("opened {hnsw_layer_nodes:?} for layer {layer_number}");
        let node_slice_u8: &[u8] = unsafe {
            let nodes: &[VectorId] = &layer.nodes;
            let ptr = nodes.as_ptr() as *const u8;
            let size = layer.nodes.len() * mem::size_of::<VectorId>();
            slice::from_raw_parts(ptr, size)
        };
        hnsw_layer_nodes_file.write_all(node_slice_u8)?;
        eprintln!("wrote nodes for layer {layer_number}");

        // Write Neighbors
        let mut hnsw_layer_neighbors: PathBuf = path.as_ref().into();
        hnsw_layer_neighbors.push(format!("layer.neighbors.{layer_number}"));
        let mut hnsw_layer_neighbors_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&hnsw_layer_neighbors)?;
        eprintln!("opened {hnsw_layer_neighbors_file:?} for layer {layer_number}");
        let neighbor_slice_u8: &[u8] = unsafe {
            let neighbors: &[NodeId] = &layer.neighbors;
            let ptr = neighbors.as_ptr() as *const u8;
            let size = layer.neighbors.len() * mem::size_of::<NodeId>();
            slice::from_raw_parts(ptr, size)
        };
        hnsw_layer_neighbors_file.write_all(neighbor_slice_u8)?;
        eprintln!("wrote neighbors for layer {layer_number}");
    }
    Ok(())
}

pub fn deserialize_hnsw<C: Serializable + Clone, P: AsRef<Path>>(
    path: P,
    params: C::Params,
) -> Result<Hnsw<C>, SerializationError> {
    let mut hnsw_meta: PathBuf = path.as_ref().into();
    hnsw_meta.push("meta");
    let mut hnsw_meta_file = OpenOptions::new().read(true).open(dbg!(hnsw_meta))?;
    let mut contents = String::new();
    hnsw_meta_file.read_to_string(&mut contents)?;
    let HNSWMeta {
        layer_count,
        build_parameters,
    }: HNSWMeta = serde_json::from_str(&contents)?;

    let mut hnsw_comparator_path: PathBuf = dbg!(path.as_ref().into());
    hnsw_comparator_path.push("comparator");

    // If we don't have a comparator, the HNSW is empty
    if !hnsw_comparator_path.exists() {
        return Err(SerializationError::IndexNotFound);
    }
    let comparator: C = C::deserialize(&hnsw_comparator_path, params)?;
    let mut layers = Vec::with_capacity(layer_count);
    for i in 0..layer_count {
        let layer_number = layer_count - i - 1;
        // Read meta database_
        let mut hnsw_layer_meta: PathBuf = path.as_ref().into();
        hnsw_layer_meta.push(format!("layer.meta.{layer_number}"));
        let mut hnsw_layer_meta_file: std::fs::File =
            OpenOptions::new().read(true).open(dbg!(hnsw_layer_meta))?;
        let mut contents = String::new();
        hnsw_layer_meta_file.read_to_string(&mut contents)?;
        let LayerMeta {
            node_count,
            neighborhood_size,
        } = serde_json::from_str(&contents)?;

        let mut hnsw_layer_nodes: PathBuf = path.as_ref().into();
        hnsw_layer_nodes.push(format!("layer.nodes.{layer_number}"));
        let mut hnsw_layer_nodes_file: std::fs::File =
            OpenOptions::new().read(true).open(dbg!(hnsw_layer_nodes))?;
        let mut nodes: Vec<VectorId> = Vec::with_capacity(node_count);
        #[allow(clippy::uninit_vec)]
        unsafe {
            nodes.set_len(node_count);
        }
        let size = node_count * mem::size_of::<VectorId>();
        let node_slice_u8: &mut [u8] = unsafe {
            let nodes: &mut [VectorId] = &mut nodes;
            let ptr = nodes.as_mut_ptr() as *mut u8;
            slice::from_raw_parts_mut(ptr, size)
        };
        hnsw_layer_nodes_file.read_exact(node_slice_u8)?;

        let mut hnsw_layer_neighbors: PathBuf = path.as_ref().into();
        hnsw_layer_neighbors.push(format!("layer.neighbors.{layer_number}"));
        let mut hnsw_layer_neighbors_file: std::fs::File = OpenOptions::new()
            .read(true)
            .open(dbg!(hnsw_layer_neighbors))?;
        let mut neighbors: Vec<NodeId> = Vec::with_capacity(node_count * neighborhood_size);
        #[allow(clippy::uninit_vec)]
        unsafe {
            neighbors.set_len(node_count * neighborhood_size);
        }
        let neighbor_slice_u8: &mut [u8] = unsafe {
            let neighbors: &mut [NodeId] = &mut neighbors;
            let ptr = neighbors.as_mut_ptr() as *mut u8;
            let size = (node_count * neighborhood_size) * mem::size_of::<NodeId>();
            slice::from_raw_parts_mut(ptr, size)
        };
        hnsw_layer_neighbors_file.read_exact(neighbor_slice_u8)?;

        layers.push(Layer {
            comparator: comparator.clone(),
            neighborhood_size,
            neighbors,
            nodes,
        });
    }
    Ok(Hnsw {
        layers,
        build_parameters,
    })
}
