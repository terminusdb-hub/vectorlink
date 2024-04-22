class ModelBackend:
    def process_chunk(self, strings, fp):
        array = self.process_chunk_to_array(strings)
        array.tofile(fp)
