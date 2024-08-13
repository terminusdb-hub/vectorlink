import ctypes
import struct
import argparse

class Pair(struct.Struct):
    __fields__ = [('id', ctypes.c_ulong),
                  ('distance', ctypes.c_float)]

def read_offset(istream):
    return struct.unpack("L", istream)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-prefix', help='input match file prefix (before .idx or .match) to interpret', required=True)
    parser.add_argument('-o', '--output-file', help='output file for reordered match', required=True)

    args = parser.parse_args()

    # 1. First, load match file.
    input_prefix = args.input_prefix
    input_file = f"{input_prefix}.queues"
    input_index = f"{input_prefix}.index"

    pair_size = sizeof(Pair)
    result = []
    with open(input_index, 'rb') as idx:
        idx_buf = idx.read()
        ulongs_in_file = idx_buf.len() / 8
        with open(input_file, 'rb') as i:
            file_buf = i.read()
            for i in range(0, ulongs_in_file):
                start = struct.unpack_from("<L", i * 8)
                end = struct.unpack_from("<L", (i+1) * 8)
                size = int( (end - start) / pair_size)
                array = (Pair * size).from_buffer(file_buf, start)
                result.append(array)

    print(result)
    # 2. Prescan vectors for loading from the match file
    #    * requires offset calculation for match vector

    # 3. Preload the vectors into the GPU

    # 4. Perform match calculations and write the output matches as binary structs
    #
    # The match calculation is a dot product of the match vectors and the candidate
    # queue
