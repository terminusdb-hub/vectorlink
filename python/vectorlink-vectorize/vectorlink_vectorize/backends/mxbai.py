from sentence_transformers import SentenceTransformer

from .model import ModelBackend

class MxbaiBackend(ModelBackend):
    def __init__(self):
        self.model = SentenceTransformer("mixedbread-ai/mxbai-embed-large-v1").cuda()

    def process_chunk_to_array(self, strings):
        return self.model.encode(strings)
