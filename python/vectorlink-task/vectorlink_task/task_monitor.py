#!/usr/bin/env python
import argparse
import etcd3
import json
import threading
import os
from queue import Queue

etcd = None
CLAIMS = '/services/claims/'
TASKS = '/services/tasks/'
QUEUE = '/services/queue/'
INTERRUPT = '/services/interrupt/'

def task_to_claim(task):
    task_id = task[len(TASKS):]
    return f'{CLAIMS}{task_id}'

def task_to_queue(task):
    task_id = task[len(TASKS):]
    return f'{QUEUE}{task_id}'

def task_to_interrupt(task):
    task_id = task[len(TASKS):]
    return f'{INTERRUPT}{task_id}'

def claim_to_task(claim):
    task_id = claim[len(CLAIMS):]
    return f'{TASKS}{task_id}'

def runnable_status(status):
    return status in ['pending', 'resuming']

def iterator_to_queue(it, q):
    for v in it:
        q.put(v)

def pause_if_orphan(task_key):
    claim = task_to_claim(task_key)
    queue = task_to_queue(task_key)
    interrupt = task_to_interrupt(task_key)
    (v,_) = etcd.get(task_key)
    state = json.loads(v)

    if state['status'] == 'running':
        # set to resuming instead of paused for quick repickup
        state['status'] = 'resuming'
        etcd.transaction(
            compare=[
                etcd.transactions.value(task_key) == v,
                etcd.transactions.version(claim) == 0, # this should always be true if the above is true, but let's check anyway
            ],
            success=[
                etcd.transactions.put(task_key, json.dumps(state)),
                etcd.transactions.delete(interrupt) # these can't be any good
            ],
            failure=[]
        )

def enqueue(task_key):
    claim = task_to_claim(task_key)
    queue = task_to_queue(task_key)
    # requeue stuff
    print(f'enqueue {queue}')
    etcd.transaction(
        compare=[
            etcd.transactions.version(claim) == 0,
            etcd.transactions.version(queue) == 0,
        ],
        success=[
            etcd.transactions.put(queue, '')
        ],
        failure=[]
    )

def main():
    global etcd
    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server')
    args = parser.parse_args()
    host = args.etcd
    if host is None:
        host = os.getenv('ETCD_HOST')
    if host is None:
        etcd = etcd3.client()
    else:
        etcd = etcd3.client(host=host)

    (tasks_watch, tasks_watch_cancel) = etcd.watch_prefix(TASKS)
    (claims_watch, claims_watch_cancel) = etcd.watch_prefix(CLAIMS)
    q = Queue()
    tasks_watch_thread = threading.Thread(target=iterator_to_queue, args=(tasks_watch, q))
    claims_watch_thread = threading.Thread(target=iterator_to_queue, args=(claims_watch, q))
    tasks_watch_thread.start()
    claims_watch_thread.start()
    try:
        result = etcd.get_prefix(TASKS, sort_order='ascend', sort_target='create')
        for (v, kv) in result:
            state = json.loads(v)
            if state['status'] == 'running':
                pause_if_orphan(kv.key.decode('utf-8'))
            elif runnable_status(state['status']):
                print(kv)
                task_key = kv.key.decode('utf-8')
                enqueue(task_key)

        # Now that any stragglers are cleared up, it is time to start relying on the watch
        while True:
            event = q.get()
            # is it a disappearing claim?
            if isinstance(event, etcd3.events.DeleteEvent):
                key = event.key.decode('utf-8')
                if key.startswith(CLAIMS):
                    task_key = claim_to_task(key)
                    pause_if_orphan(task_key)

            # is it a new task?
            elif isinstance(event, etcd3.events.PutEvent):
                key = event.key.decode('utf-8')
                if key.startswith(TASKS):
                    state = json.loads(event.value)
                    if runnable_status(state['status']):
                        enqueue(key)

    finally:
        # todo proper cleanup here
        pass

if __name__ == '__main__':
    main()
