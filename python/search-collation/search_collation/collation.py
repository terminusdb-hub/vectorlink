import struct
import argparse
import sys
import numpy

def read_offset(istream):
    return struct.unpack("L", istream)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-prefix', help='input match file prefix (before .idx or .match) to interpret', required=True)
    parser.add_argument('-o', '--output-file', help='output file for reordered match', required=True)
    parser.add_argument('-d', '--directory', help='vector files directory', required=True)
    args = parser.parse_args()

    # 1. First, load match file.

    input_prefix = args.input_prefix
    input_file = f"{input_prefix}.queues"
    input_index = f"{input_prefix}.index"

    pair_size = struct.calcsize("<Lf")
    # print(f"pair size: {pair_size}")
    # sys.exit(0)
    result = {}
    with open(input_index, 'rb') as idx:
        idx_buf = idx.read()
        ulongs_in_file = int(len(idx_buf) / 8)
        with open(input_file, 'rb') as ifile:
            for i in range(0, ulongs_in_file - 1):
                start = struct.unpack_from("<L", idx_buf, i * 8)[0]
                end = struct.unpack_from("<L", idx_buf, (i+1) * 8)[0]
                size = int( (end - start) / pair_size)
                if size == 0:
                    pass
                queue_buf = ifile.read(size)
                array = struct.iter_unpack("<Lf", queue_buf)
                result[i] = []
                for (vid, _) in list(array):
                    result[i].append(vid)

    # 2. Prescan vectors for loading from the match file
    #    * requires offset calculation for match vector (but not for 0)
    ids = []
    for key in result:
        ids.append(key)
        for i in result[key]:
            ids.append(i)

    ids.sort()

    # 3. Preload the vectors into the GPU
    vector_file_size = 128370618368
    vector_size = 1536 * 4 # dimension * f32
    vector_file_count = vector_file_size / vector_size

    file_no = 0
    f = open(f"{directory}/{file_no}.vecs", 'rb')
    vecs = numpy.array([])
    for i in ids:
        new_file_no = int(i / vector_file_count)
        if new_file_no != file_no:
            file_no = new_file_no
            f.close()
            f = open(f"{directory}/{file_no}.vecs", 'rb')
        file_offset = i % file_size * file_no
        f.seek(file_offset * vector_size)
        raw_buf = f.read(vector_size)
        vecs.append(numpy.frombuffer(raw_buf, dtype=float))

    f.close()
    # 4. Perform match calculations and write the output matches as binary structs
    #
    # The match calculation is a dot product of the match vectors and the candidate
    # queue
