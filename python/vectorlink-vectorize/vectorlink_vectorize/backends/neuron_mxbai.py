from sentence_transformers import SentenceTransformer
from .model import ModelBackend
import torch
import torch_neuronx
from os import path, environ

MODEL_FILENAME = "mxbai_embed_large_v1_neuron.pt"
MODELS_CACHE_DIR_PATH = environ['INFERENTIA_MODELS'] + "/" + MODEL_FILENAME
# ^ throws an error if filepath does not exist

class NeuronMxbaiBackend(ModelBackend):    
    def __init__(self):
        if not path.exists(MODELS_CACHE_DIR_PATH):
            self.neuron_compile()
        self.model = torch.jit.load(MODELS_CACHE_DIR_PATH)

    def process_chunk_to_array(self, strings):
        return self.model.encode(strings)
    
    def neuron_compile():
        model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1")
        neuron_model = torch_neuronx.trace(model, None)
        neuron_model.save(MODEL_FILENAME)
        
if __name__ == '__main__':
    neuron_mxbai_backend = NeuronMxbaiBackend()
    neuron_mxbai_backend.neuron_compile()