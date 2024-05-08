class ModelBackend:
    def process_chunk(self, strings):
        array = self.process_chunk_to_array(strings)
        return array.tobytes()
