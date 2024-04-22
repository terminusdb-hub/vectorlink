from vectorlink_task.etcd_task import TaskQueue, TaskInterrupted
from vectorlink_vectorize import vectorize
import sys
import json
import socket
import argparse
import os
import traceback
from collections import deque
from datetime import datetime

identity = None
directory = None
chunk_size = 100

def retrieve_identity():
    from_env = os.getenv('VECTORIZER_IDENTITY')
    return from_env if from_env is not None else socket.getfqdn()

def resolve_path(path):
    rootdir = os.path.abspath(directory)
    normalized = os.path.normpath(f'{rootdir}/{path}')
    if not normalized.startswith(rootdir):
        raise ValueError(f'path {path} is invalid')

    return normalized

def start_(task, truncate=0, skip=0):
    global backend
    global chunk_size
    init = task.init()
    input_file = resolve_path(init['input_file'])
    output_file = resolve_path(init['output_file'])

    print(f"Input file: {input_file}", file=sys.stderr)
    print(f"Output file: {output_file}", file=sys.stderr)

    progress = task.progress()
    if progress is None:
        # this is the first run. lets determine how large this file is
        with open(input_file, 'r') as input_fp:
            total = sum(1 for line in input_fp)
            task.set_progress({'count': 0, 'total': total})
    else:
        total = progress['total']

    chunk = []
    count = skip

    with open(output_file, 'a+') as output_fp:
        # truncate to a safe known size
        output_fp.truncate(truncate)
        output_fp.seek(0, os.SEEK_END)

        duration_queue = deque(maxlen=10)
        with open(input_file, 'r') as input_fp:
            start_time = datetime.now()
            for line in input_fp:
                # skip already processed lines
                if skip != 0:
                    skip -= 1
                    if skip == 0:
                        # this was our last skip.
                        start_time = datetime.now()
                    continue

                json_str = json.loads(line)
                chunk.append(json_str)
                if len(chunk) == chunk_size:
                    task.alive()
                    backend.process_chunk(chunk, output_fp)
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    start_time = end_time

                    rate = chunk_size / duration

                    duration_queue.append(duration)
                    avg_rate = (len(duration_queue) * chunk_size) / sum(duration_queue)

                    count += len(chunk)
                    task.set_progress({'count': count, 'total': total, 'rate': rate, 'avg_rate': avg_rate})
                    chunk = []
        if len(chunk) != 0:
              backend.process_chunk(chunk, output_fp)
              end_time = datetime.now()
              duration = (end_time - start_time).total_seconds()
              start_time = end_time

              rate = len(chunk) / duration

              duration_queue.append(duration)
              avg_rate = ((len(duration_queue) - 1) * chunk_size + len(chunk)) / sum(duration_queue)
              count += len(chunk)
              task.set_progress({'count': count, 'total': total, 'rate': rate, 'avg_rate': avg_rate})

        output_fp.flush()
        os.fsync(output_fp.fileno())
        task.finish(count)

def start(task):
    task.start()
    try:
        start_(task)
    except TaskInterrupted as e:
        pass
    except Exception as e:
        stack_trace = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        task.finish_error(stack_trace)

def resume(task):
    # We have to figure out where we left off
    # This is determined by the current file size. rounding that down
    # to the nearest multiple of the vector size gets us a reliable
    # count. This might be lower than the number in progress!
    if task.status() == 'resuming':
        task.resume()
    init = task.init()
    size = os.path.getsize(resolve_path(init['output_file']))
    count = size // 4096
    truncate_to = count * 4096

    print(f'resuming after having already vectorized {count}', file=sys.stderr)
    progress = task.progress()
    total = None
    if progress is not None:
        total = progress.get('total')

    if total is None:
        with open(resolve_path(init['input_file']), 'r') as input_fp:
            total = sum(1 for line in input_fp)

    task.set_progress({'count': count, 'total': total})

    try:
        start_(task, truncate=truncate_to, skip=count)
    except TaskInterrupted as e:
        pass
    except Exception as e:
        task.finish_error(str(e))

def main():
    global etcd
    global directory
    global identity
    global chunk_size
    global backend

    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    parser.add_argument('--identity', help='the identity this worker will use when claiming tasks')
    parser.add_argument('--directory', help='the directory where files are to be found')
    parser.add_argument('--chunk-size', type=int, help='the amount of vectors to process at once')
    parser.add_argument('--backend', type=str, default=os.getenv('VECTORIZER_BACKEND', 'bloom'), help='the backend to use for vectorization')
    args = parser.parse_args()
    identity = args.identity if args.identity is not None else retrieve_identity()

    directory = args.directory
    if directory is None:
        directory = os.getenv('VECTORIZER_DIRECTORY')
    print(f'using directory {directory}', file=sys.stderr)

    chunk_size = args.chunk_size
    if chunk_size is None:
        chunk_size_str = os.getenv('VECTORIZER_CHUNK_SIZE')
        chunk_size = int(chunk_size_str) if chunk_size_str else 100
    print(f'using chunk size {chunk_size}', file=sys.stderr)

    etcd = args.etcd
    if etcd is None:
        etcd = os.getenv('ETCD_HOST')

    if etcd is not None:
        queue = TaskQueue('vectorizer', identity, host=etcd)
    else:
        queue = TaskQueue('vectorizer', identity)

    backend = vectorize.init_backend(args.backend)

    print('start main loop', file=sys.stderr)
    try:
        while True:
            task = queue.next_task()
            print('wow a task: ' + task.status(), file=sys.stderr)
            match task.status():
                case 'pending':
                    print('starting..', file=sys.stderr)
                    start(task)
                case 'resuming':
                    resume(task)
                case _:
                    sys.stderr.write(f'cannot process task with status {task.status()}\n')
    except SystemExit:
        pass

if __name__ == '__main__':
    main()
