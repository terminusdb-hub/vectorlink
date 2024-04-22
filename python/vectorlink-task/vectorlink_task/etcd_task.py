#!/usr/bin/env python
import etcd3
import json

class TaskStatusError(Exception):
    def __init__(self, task_id, expected_status, actual_status):
        self.task_id = task_id
        self.expected_status = expected_status
        self.actual_status = actual_status
        super().__init__(f'task {task_id} was expected to have status `{expected_status}`, but actual status was `{actual_status}`')

class TaskTimeoutError(Exception):
    def __init__(self, task_id, message="Task timed out"):
        self.task_id = task_id
        super().__init__(self.message)

class TaskInterrupted(Exception):
    def __init__(self, task_id, reason):
        self.task_id = task_id
        self.reason = reason
        super().__init__(f'task {task_id} was interrupted: {reason}')

class Task:
    def __init__(self, queue, task_id, lease=None):
        self.queue = queue
        self.task_id = task_id
        self.lease = lease
        self.task_key = f'{self.queue.tasks_prefix}{task_id}'
        self.claim_key = f'{self.queue.claims_prefix}{task_id}'
        self.interrupt_key = f'{self.queue.interrupt_prefix}{task_id}'
        self.interrupting = True # not strictly true, just don't want to trigger interrupting logic
        self.state = self._task_state()
        self.interrupting = False

    def alive(self):
        # this gets called in various places that do reads and updates
        # to notify etcd that we're still alive. It is also used to
        # check if someone wants to interrupt us.
        if self.lease.refresh()[0].TTL == 0:
            raise TaskTimeoutError(self.task_id)
        (reason,_) = self.queue.etcd.get(self.interrupt_key)
        if reason:
            # set our state to reflect the interruption
            self.interrupt(reason)
            # .. revoke our lease
            self.lease.revoke()
            # .. and raise an exception to leave whatever computation we're doing
            raise TaskInterrupted(self.task_id, reason)

    def _task_state(self):
        # We want to allow retrieval of the state even without lease,
        # but if there is a lease, let's also renew it upon retrieval
        if self.lease and not self.interrupting:
            self.alive()
        (task_string,_) = self.queue.etcd.get(self.task_key)

        return json.loads(task_string)


    def _update_task_state(self, extra_ops=[]):
        # Only set when we have the lease, so transaction to make sure.
        state_string = json.dumps(self.state)
        # setting the task state implies we're alive and kicking so let's make sure etcd knows that
        if not self.interrupting:
            self.alive()
        # this transaction will really only fail if for some weird
        # reason between the alive above and the transaction below,
        # the lease expired. This will only happen in cases of serious
        # failure of the etcd cluster, or mysterious long suspensions
        # happening to the host running this script. Transaction
        # failure here will throw an ugly error but I think the
        # ugliness fits the seriousness.
        success_ops = [
                self.queue.etcd.transactions.put(self.claim_key, self.queue.identity, lease=self.lease),
                self.queue.etcd.transactions.put(self.task_key, state_string)
            ]
        success_ops.extend(extra_ops)
        self.queue.etcd.transaction(
            compare=[],
            success=success_ops,
            failure=[])

    def status(self):
        return self.state['status']

    def _verify_status(self, expected):
        if self.state['status'] != expected:
            raise TaskStatusError(self.task_id, expected, self.state['status'])

    def _set_status(self, status):
        self.state['status'] = status
        self._update_task_state()

    def _transition_to_status(self, from_status, to_status):
        self._verify_status(from_status)
        self._set_status(to_status)

    def start(self):
        self._transition_to_status('pending', 'running')

    def finish(self, final_result):
        self.state['result'] = final_result
        self._transition_to_status('running', 'complete')
        self.lease.revoke()

    def finish_error(self, error):
        self.state['error'] = error
        self._transition_to_status('running', 'error')
        self.lease.revoke()

    def interrupt(self, reason):
        self.interrupting = True

        match reason:
            case b'cancel':
                status = 'canceled'
            case b'pause':
                status = 'paused'
            case _:
                raise ValueError(reason)

        # We need to clear the interrupt key, as well as set our own
        # status
        self._verify_status('running')
        self.state['status'] = status
        self._update_task_state(extra_ops=[self.queue.etcd.transactions.delete(self.interrupt_key)])

        self.interrupting = False

    def resume(self):
        self._transition_to_status('resuming', 'running')

    def init(self):
        return self.state.get('init')

    def progress(self):
        return self.state.get('progress')

    def set_progress(self, progress):
        self._verify_status('running')
        self.state['progress'] = progress
        self._update_task_state()

    def complete(self):
        return self.state.get('complete')

    def error(self):
        return self.state.get('error')

def runnable_status(status):
    return status in ['pending', 'running', 'resuming']

class TaskQueue:
    def __init__(self, service_name, identity, **kwargs):
        self.service_name = service_name
        self.identity = identity
        self.etcd = etcd3.client(**kwargs)
        self.queue_prefix = f'/services/queue/{service_name}/'
        self.tasks_prefix = f'/services/tasks/{service_name}/'
        self.claims_prefix = f'/services/claims/{service_name}/'
        self.interrupt_prefix = f'/services/interrupt/{service_name}/'

    def queue_key_to_task_id(self, queue_key):
        queue_key = queue_key.decode('utf-8')
        return queue_key[len(self.queue_prefix):]

    def get_task(self, task_id):
        # todo check that task actually exists
        return Task(self, task_id)

    def claim_task(self, task_id, ttl=10):
        queue_key = f'{self.queue_prefix}{task_id}'
        task_key = f'{self.tasks_prefix}{task_id}'
        claim_key = f'{self.claims_prefix}{task_id}'

        lease = self.etcd.lease(ttl)
        (result,_) = self.etcd.transaction(
            compare=[
                # make sure that our task is unclaimed
                self.etcd.transactions.version(claim_key) == 0
            ],
            success=[
                # dequeue and set claim on task
                self.etcd.transactions.delete(queue_key),
                self.etcd.transactions.put(claim_key, self.identity, lease=lease)
            ],
            failure=[
                # delete superfluous queue item if it just happens to be here
                self.etcd.transactions.delete(queue_key),
            ])
        if result:
            task = Task(self, task_id, lease)
            if runnable_status(task.status()):
                return Task(self, task_id, lease)

        # explicit default
        return None

    def next_task(self):
        # we start out be setting up a watch, cause if we wait until after our query we're potentially gonna have a race condition
        # this is really an issue with this particular library. the underlying protocol can restart watches from a known revision.

        (watch, watch_cancel) = self.etcd.watch_prefix(self.queue_prefix)
        try:
            result = self.etcd.get_prefix(self.queue_prefix, sort_order='ascend', sort_target='create')
            for (_, kv) in result:
                task_id = self.queue_key_to_task_id(kv.key)
                task = self.claim_task(task_id)
                if task:
                    return task

            # well, it wasn't there. let's wait for one to pop up
            for event in watch:
                if isinstance(event, etcd3.events.PutEvent):
                    task_id = self.queue_key_to_task_id(event.key)
                    task = self.claim_task(task_id)
                    if task:
                        return task
        finally:
            self.etcd.cancel_watch(watch_cancel)
