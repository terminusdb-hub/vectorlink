import struct
import argparse
import sys
import numpy
import torch

def cosine_distance(X, i, ids):
    m = torch.index_select(X,ids)
    v = torch.index_select(X,i)
    vT = torch.transpose(v)
    d = torch.matmul(vT, m)
    m_norms = torch.norm(x, dim=1)
    v_norm = torch.norm(v, dim=1).item()
    return d / (m_norms * v_norm)

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

    pair_size = struct.calcsize("<Qf")
    print(f"pair size: {pair_size}")
    ulong_size = struct.calcsize("<Q")
    print(f"ulong size: {ulong_size}")
    # sys.exit(0)
    result = {}
    with open(input_index, 'rb') as idx:
        idx_buf = idx.read()
        ulongs_in_file = int(len(idx_buf) / ulong_size)
        with open(input_file, 'rb') as ifile:
            for i in range(0, ulongs_in_file - 1):
                start = struct.unpack_from("<Q", idx_buf, i * ulong_size)[0]
                end = struct.unpack_from("<Q", idx_buf, (i+1) * ulong_size)[0]
                #print(f"range: {end}-{start}")
                size = int((end - start))
                if size == 0:
                    continue
                queue_buf = ifile.read(size)
                # Do I need this extra f for alignment?
                array = struct.iter_unpack("<Qff", queue_buf)
                result[i] = []
                for (vid, _, _) in list(array):
                    result[i].append(vid)

    # 2. Prescan vectors for loading from the match file
    #    * requires offset calculation for match vector (but not for 0)
    ids = []
    for key in result:
        ids.append(key)
        for i in result[key]:
            ids.append(i)

    ids.sort()

    id_map = {}
    for i in range(0,len(ids)):
        id_map[ids[i]] = i

    # 3. Preload the vectors into the GPU
    vector_file_size = 128370618368
    f32_size = struct.calcsize("<f")
    vector_size = int(1024 * f32_size) # dimension * f32
    vector_file_count = int(vector_file_size / vector_size)

    file_no = 0
    f = open(f"{args.directory}/{file_no}.vecs", 'rb')
    buf = b''
    count = 0
    for i in ids:
        new_file_no = int(i / vector_file_count)
        if new_file_no != file_no:
            break
            # Only comparing against ourselves
            #file_no = new_file_no
            #f.close()
            #f = open(f"{directory}/{file_no}.vecs", 'rb')
        file_offset = i % vector_file_count * file_no
        f.seek(file_offset * vector_size)
        raw_buf = f.read(vector_size)
        buf += raw_buf
        count += 1
        if count > 10:
            break

    f.close()
    # 4. Perform match calculations and write the output matches as binary structs
    #
    # The match calculation is a dot product of the match vectors and the candidate
    # queue

    with torch.device("cuda"):
        X = torch.frombuffer(buf, dtype=torch.float32)
        X.reshape([len(10), 1024]) # X.reshape([len(ids), 1024])
        compiled_cosine = torch.compile(cosine_distance)
        for i in result:
            ids = torch.tensor(result[key])
            offsets = torch.tensor(list(map(lambda i: id_map[i])))
            v_i = torch.tensor([id_map[i]])
            results = compiled_cosign(X, v_i, offsets)
            print(f"i: {i} ids: {ids} results: {results}")
            break
