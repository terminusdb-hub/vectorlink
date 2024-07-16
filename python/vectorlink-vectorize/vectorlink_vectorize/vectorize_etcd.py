from vectorlink_task import Queue
from vectorlink_vectorize import vectorize
import sys
import json
import socket
import argparse
import os
import traceback
from collections import deque
from datetime import datetime
import boto3
import pybars

import struct

identity = None
chunk_size = 100
segment_size = 25000;

s3 = boto3.client('s3')

def retrieve_identity():
    from_env = os.getenv('VECTORIZER_IDENTITY')
    return from_env if from_env is not None else socket.getfqdn()

def byte_offset_for_line_number(bucket_name, index_key, line_number):
    offset_in_index = line_number * 8
    r = f'bytes={offset_in_index}-{offset_in_index+7}'
    print(f'range in byte index: {r}', file=sys.stderr)

    response=s3.get_object(
        Bucket=bucket_name,
        Key=index_key,
        Range=r
    )
    data = response['Body'].read()
    return struct.unpack('<q', data)[0]

def start_(task):
    global backend
    global chunk_size
    print("got to this point")
    init = task.init
    print("2 got to this point")
    # bucket name
    # strings input key
    # newline-index input key
    # vector output key
    # range
    # template
    bucket_name = init['bucket_name']
    strings_key = init['strings_key']
    newline_index_key = init['newline_index']
    output_key = init['output_key']
    start_line = int(init['start_line'])
    end_line = int(init['end_line'])
    n_strings = end_line - start_line + 1


    template_string = init['template']
    template = pybars.Compiler().compile(template_string)

    progress = task.progress
    if progress is None:
        # first run. let's start a multipart upload
        upload_id = s3.create_multipart_upload(
            Bucket=bucket_name,
            Key=output_key)['UploadId']

        progress = {'count': 0, 'upload_id': upload_id, 'parts':[]}
        task.progress = progress
    else:
        upload_id = progress['upload_id']

    start_byte = byte_offset_for_line_number(bucket_name, newline_index_key, start_line + progress['count'])
    end_byte = byte_offset_for_line_number(bucket_name, newline_index_key, end_line + 1) - 1

    print(f'start byte: {start_byte} end byte: {end_byte}', file=sys.stderr)

    chunk = []

    embeddings_queued = 0
    part_number = 1
    prepared_part = bytearray()
    obj = s3.get_object(Bucket=bucket_name, Key=strings_key, Range=f'bytes={start_byte}-{end_byte}')
    count = 0
    for line in obj['Body'].iter_lines():
        if line == b'':
            if count == 0:
                continue
            else:
                raise Exception(f'unusual empty line @ {count}')

        count += 1

        try:
            j = json.loads(line)
        except json.JSONDecodeError as e:
            raise Exception(f'invalid json line: {line}')

        string = template(j)
        chunk.append(string)

        # It doesn't make sense to embed each string individually. We
        # collect chunk_size strings to embed at once to amortize the
        # cost of sending stuff to the gpu.
        if len(chunk) == chunk_size:
            task.alive()
            result = backend.process_chunk(chunk)
            task.alive()
            prepared_part.extend(result)
            chunk = []
            embeddings_queued += chunk_size

        # Upload parts in an S3 multipart upload have to be at least
        # 5MB (except for the final part). We therefore hold off from
        # uploading a part until we have that much.
        if embeddings_queued >= segment_size:
            result = s3.upload_part(
                Bucket=bucket_name,
                Key=output_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=prepared_part
            )

            etag = result['ETag']
            progress['parts'].append({'PartNumber':part_number, 'ETag': etag})
            progress['count'] += embeddings_queued
            task.progress = progress
            prepared_part.clear()
            part_number += 1
            embeddings_queued = 0

    # Do a final embedding and upload for the remainder
    if len(chunk) != 0:
        result = backend.process_chunk(chunk)
        prepared_part.extend(result)
        embeddings_queued += len(chunk)
    if embeddings_queued >= 0:
        result = s3.upload_part(
            Bucket=bucket_name,
            Key=output_key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=prepared_part
        )

        etag = result['ETag']
        progress['parts'].append({'PartNumber':part_number, 'ETag': etag})
        progress['count'] += embeddings_queued
        task.progress = progress

    response = s3.complete_multipart_upload(
        Bucket=bucket_name,
        Key=output_key,
        MultipartUpload={'Parts': progress['parts']},
        UploadId = upload_id)

    task.finish(progress['count'])

def start(task):
    task.start()
    try:
        start_(task)
    except Exception as e:
        stack_trace = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        task.finish_error(stack_trace)

def resume(task):
    task.resume()
    try:
        start_(task)
    except Exception as e:
        task.finish_error(str(e))

def main():
    global etcd
    global identity
    global chunk_size
    global backend

    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    parser.add_argument('--identity', help='the identity this worker will use when claiming tasks')
    parser.add_argument('--chunk-size', type=int, help='the amount of vectors to process at once')
    parser.add_argument('--backend', type=str, default=os.getenv('VECTORIZER_BACKEND', 'bloom'), help='the backend to use for vectorization')
    args = parser.parse_args()
    identity = args.identity if args.identity is not None else retrieve_identity()

    chunk_size = args.chunk_size
    if chunk_size is None:
        chunk_size_str = os.getenv('VECTORIZER_CHUNK_SIZE')
        chunk_size = int(chunk_size_str) if chunk_size_str else 100
    print(f'using chunk size {chunk_size}', file=sys.stderr)

    etcd = args.etcd
    if etcd is None:
        etcd = os.getenv('ETCD_HOST')

    if etcd is not None:
        queue = Queue([etcd], f'vectorizer/{args.backend}', identity)
    else:
        queue = Queue(["localhost:2379"], f'vectorizer/{args.backend}', identity)

    backend = vectorize.init_backend(args.backend)

    print('start main loop', file=sys.stderr)
    try:
        while True:
            task = queue.next()
            print('picking up task: ' + task.id, file=sys.stderr)
            match task.status:
                case 'pending':
                    print(f'starting task {task.id}..', file=sys.stderr)
                    start(task)
                case 'resuming':
                    resume(task)
                case _:
                    sys.stderr.write(f'cannot process task with status {task.status}\n')
    except SystemExit:
        pass

if __name__ == '__main__':
    main()
