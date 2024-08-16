import struct
import argparse
import sys
import numpy
import torch
import csv
import json

def get_offsets(data, i):
    position = i * 8
    start = struct.unpack_from('<Q', data, position)[0]
    end = struct.unpack_from('<Q', data, position + 8)[0]
    return (start, end)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input-prefix', help='input match file prefix (before .idx or .match) to interpret', required=True)
    parser.add_argument('-o', '--output-file', help='output file for reordered match', required=True)
    parser.add_argument('-d', '--directory', help='vector files directory', required=True)
    parser.add_argument('-f', '--full', help='use full vector distances', action='store_true', default=False)
    parser.add_argument('-t', '--threshold', help='threshold value to use to chop distance', type=float)
    parser.add_argument('-r', '--report-type', help='the type of report (one of: csv, binary)', choices=['csv', 'binary'], default='csv')
    parser.add_argument('-l', '--lines', help='lines file with the actual data', required=True)
    parser.add_argument('-x', '--index', help='lines index file', required=True)
    args = parser.parse_args()


    threshold = float('inf')
    if args.threshold:
        threshold = args.threshold

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
                for (vid, distance, _) in list(array):
                    if distance < threshold:
                        #print(f"distance: {distance}")
                        result[i].append(vid)

    if not args.full:
        # 2. Alternative branch: we do not need to reorder and can directly output the appropriate matches
        # to get real row_id file_id we need to load the whole thing into memory
        f = open(args.lines, 'rb')
        data = f.read()
        x = open(args.index, 'rb')
        offsets = x.read()

        o = open(args.output_file, 'w')
        writer = csv.writer(o)
        for i in result:
            (i_start, i_end) = get_offsets(offsets, i)
            #print(f"i start: {i_start} i_end: {i_end}")
            i_json = json.loads(data[i_start:i_end])
            i_dfi = i_json['DATAFILE_ID']
            i_ri = i_json['ROW_ID']
            for j in result[i]:
                (j_start, j_end) = get_offsets(offsets, j)
                j_json = json.loads(data[j_start:j_end])
                j_dfi = j_json['DATAFILE_ID']
                j_ri = j_json['ROW_ID']

                writer.writerow([i_dfi,i_ri,j_dfi,j_ri])
        sys.exit(0)


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
    buf = bytearray(b'')
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
        print(f"vector size: {vector_size}")
        raw_buf = f.read(vector_size)
        print(f"raw buf length: {len(raw_buf)}")
        buf += raw_buf
        count += 1
        if count >= 10:
            break

    f.close()
    # 4. Perform match calculations and write the output matches as binary structs
    #
    # The match calculation is a dot product of the match vectors and the candidate
    # queue

    torch.device("cuda")
    import torch._dynamo as dynamo
    torch._dynamo.config.verbose = True
    torch.backends.cudnn.benchmark = True

    def cosine_distance(X, i, ids):
        m = torch.index_select(X,0,ids)
        mT = torch.transpose(m, 0, 1)
        v = torch.index_select(X,0,i)
        d = torch.matmul(v, mT)
        m_norms = torch.norm(m, dim=1)
        v_norm = torch.norm(v, dim=1)
        cosine = d / (m_norms * v_norm)
        return ( (cosine - 1) / -2)

    X = torch.frombuffer(buf, dtype=torch.float32)
    X = X.reshape([10, 1024]) # X.reshape([len(ids), 1024])
    compiled_cosine = torch.compile(cosine_distance, mode="max-autotune", fullgraph=True)
    for i in result:
        ids = result[key]
        I = torch.tensor(list(map(lambda i: id_map[i], ids)))
        v_i = torch.tensor([id_map[i]])
        results = compiled_cosine(X, v_i, I)
        print(f"i: {i} ids: {ids} results: {results}")
        break
