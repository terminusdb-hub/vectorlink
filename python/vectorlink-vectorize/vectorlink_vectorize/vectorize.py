import argparse
import sys
import json

from vectorlink_vectorize.backends.bloom import BloomBackend
from vectorlink_vectorize.backends.mxbai import MxbaiBackend

def init_backend(name):
    match name:
        case "bloom":
            return BloomBackend()
        case "mxbai":
            return MxbaiBackend()
        case _:
            raise Exception(f'unknown backend {args.backend}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='input file to vectorize')
    parser.add_argument('output_file', help='output file to write vectors in')
    parser.add_argument('--backend', help='backend to use')
    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file
    backend = init_backend(args.backend)

    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    chunk = []
    with open(output_file, 'w') as output_fp:
        with open(input_file, 'r') as input_fp:
            for line in input_fp:
                json_str = json.loads(line)
                chunk.append(json_str)
                if len(chunk) == 100:
                  backend.process_chunk(chunk, output_fp)
                  chunk = []
        if len(chunk) != 0:
              backend.process_chunk(chunk, output_fp)
