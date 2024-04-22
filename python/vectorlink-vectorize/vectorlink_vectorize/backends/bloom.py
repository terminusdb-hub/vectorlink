import sys
import torch
import json
from transformers import AutoModel, AutoTokenizer, BloomModel

from .model import ModelBackend

boq, eoq, bod, eod = '[BOQ]', '[EOQ]', '[BOD]', '[EOD]'
class BloomBackend(ModelBackend):
    def __init__(self):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.cpu_device = torch.device("cpu")
        print(f"Using device: {self.device}")

        ### Initial version copied from model readme
        print("loading tokenizer")
        self.tokenizer = AutoTokenizer.from_pretrained('izhx/udever-bloom-560m', device_map='auto')
        print(self.tokenizer)
        print("loading model")
        #model = AutoModel.from_pretrained('izhx/udever-bloom-560m', device_map='auto').cuda()
        self.model = BloomModel.from_pretrained('izhx/udever-bloom-560m', device_map='auto').cuda()
        print(self.model)
        print("loaded")

        self.eoq_id, self.eod_id = self.tokenizer.convert_tokens_to_ids([eoq, eod])

        if self.tokenizer.padding_side != 'left':
            print('!!!', self.tokenizer.padding_side)
            self.tokenizer.padding_side = 'left'


    def encode(self, texts: list, is_query: bool = False, max_length=2048):
        bos = boq if is_query else bod
        eos_id = self.eoq_id if is_query else self.eod_id
        texts = [bos + t for t in texts]
        encoding = self.tokenizer(
            texts, truncation=True, max_length=max_length - 1, padding=True
        )
        for ids, mask in zip(encoding['input_ids'], encoding['attention_mask']):
            ids.append(eos_id)
            mask.append(1)
        inputs = self.tokenizer.pad(encoding, return_tensors='pt')
        inputs = inputs.to(self.device)
        with torch.inference_mode():
            outputs = self.model(**inputs)
            embeds = outputs.last_hidden_state[:, -1]
        return embeds

    def process_chunk_to_array(self, strings):
        tensor = self.encode(strings)
        tensor = tensor.to(self.cpu_device)
        return tensor.numpy().astype('float32')
