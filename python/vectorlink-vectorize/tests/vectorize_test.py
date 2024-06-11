import numpy as np
from vectorlink_vectorize.backends.mxbai import MxbaiBackend

def distance(v1, v2, distance_type):
    match distance_type:
        case 'euclidean':
            return np.linalg.norm(v1-v2)
        case 'cosine':
            return (1-np.dot(v1, v2) / np.linalg.norm(v1) / np.linalg.norm(v2))/2
        case 'manhattan':
            return np.sum(np.abs(v1 - v2))


def test_embedding():
    test_strings = ["James Henry Thompson", "Jim Thompson"]
    mxbai_backend = MxbaiBackend()
    embeddings = mxbai_backend.process_chunk_to_array(test_strings)
    
    euclidean_distance = distance(embeddings[0],embeddings[1], 'euclidean') 
    cosine_distance = distance(embeddings[0], embeddings[1], 'cosine')
    manhattan_distance = distance(embeddings[0], embeddings[1], 'manhattan')
    
    print("Embeddings:", embeddings)
    print(f'euclidean_distance: {euclidean_distance}')
    print(f'cosine_distance: {cosine_distance}')
    print(f'manhattan_distance: {manhattan_distance}')


if __name__ == '__main__':
    test_embedding()
