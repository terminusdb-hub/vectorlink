import os
import argparse
import numpy
from vectorlink_vectorize import vectorize

def distance(v1, v2, distance_type):
    match distance_type:
        case 'euclidean':
            return numpy.linalg.norm(v1-v2)
        case 'cosine':
            return (1-numpy.dot(v1, v2) / numpy.linalg.norm(v1) / numpy.linalg.norm(v2))/2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--backend', type=str, default=os.getenv('VECTORIZER_BACKEND', 'bloom'), help='the backend to use for vectorization')
    subparsers = parser.add_subparsers(dest='subcommand')
    compare = subparsers.add_parser('compare')
    compare.add_argument('--distance', type=str, default='cosine')

    compare.add_argument('first', type=str)
    compare.add_argument('second', type=str)
    args = parser.parse_args()

    match args.subcommand:
        case 'compare':
            backend = vectorize.init_backend(args.backend)
            chunk = [args.first, args.second]
            array = backend.process_chunk_to_array(chunk)
            print(distance(array[0],array[1], args.distance))
        case _:
            parser.print_help()
