"""
Apium application container
"""
import os
import socket
import asyncio
import types
import logging
import importlib
from zope.interface import implementer

from . import registry
from .interfaces import IApium, IWorker, IBroker, ISerializer
from .task import TaskRegistry, Task

log = logging.getLogger(__name__)


@implementer(IApium)
class Apium:
    """
    The Apium implementation.
    """

    def __init__(self):
        self._broker = self._worker = None
        self.settings = {'broker_url': 'amqp://localhost/',
                         'max_workers': 4,
                         'result_queue_format': '&apium-{hostname}-{pid}'}
        self._serializer = registry.get(ISerializer)()
        self._task_registry = TaskRegistry()
        self._working_queues = []
        self._result_queue = None

    def get_result_queue(self):
        """ Return the result queue name to use when sending task """
        if not self._result_queue:
            self._result_queue = self.settings['result_queue_format'].format(
                pid=os.getpid(), hostname=socket.gethostname())
        return self._result_queue

    def configure_queues(self, default_queue='#master', queues=None):
        self._task_registry.configure_queues(default_queue, queues)

    def get_task(self, task_name):
        return self._task_registry.get(task_name)

    def get_queue(self, task_name):
        return self._task_registry.get_queue(task_name)

    def _assert_broker(self):
        if self._broker is None:
            log.error('Not connected to the broker')
            raise RuntimeError('Not connected')

    @asyncio.coroutine
    def connect_broker(self):
        """ Connect to the broker server, and create its own result queue.

        Every apium application gets it own result queue."""
        log.info('Connect to the broker {}'
                 ''.format(self.settings['broker_url']))
        if self._broker is not None:
            raise RuntimeError('Already connected')

        self._working_queues = set(self._task_registry.queues.keys())
        self._broker = registry.get(IBroker)(self)
        try:
            yield from asyncio.wait_for(self._broker.connect(self.settings['broker_url']), 30)
        except Exception as exc:
            log.info('Cannot connect to the broker: {}'.format(exc))
            return False

        yield from self._broker.create_queue(self.get_result_queue())
        return True

    @asyncio.coroutine
    def disconnect_broker(self):
        """ Disconnect to the broker server. """
        if not self._broker or not self._broker.connected:
            return
        yield from self._broker.delete_queue(self.get_result_queue())
        yield from self._broker.disconnect()
        self._broker = None

    @asyncio.coroutine
    def stop(self):
        yield from self.disconnect_broker()
        if self._worker is not None:
            self._worker.stop()

    def create_worker(self):
        """ Create the backend worker"""
        if self._worker is not None:
            raise RuntimeError("Worker already spawned")
        self._worker = registry.get(IWorker)(self)
        return self._worker

    @asyncio.coroutine
    def create_queue(self, queue):
        """ Create a working queue where tasks will be pushed """
        self._assert_broker()
        self._working_queues.add(queue)
        yield from self._broker.create_queue(queue)

    @asyncio.coroutine
    def push_task(self, task_request):
        """ Create a queue wich is used to push the result. """
        self._assert_broker()
        log.info('Pushing task {} [{}]'
                 ''.format(task_request.task_name, task_request.uuid))
        message = self._serializer.serialize(task_request.to_dict())
        queue = self.get_queue(task_request.task_name)
        result = yield from self._broker.publish_message(message, queue)
        return result

    @asyncio.coroutine
    def push_result(self, task_request, task_response):
        """ push the result in the created queue. """
        log.info('Pushing the result...')
        self._assert_broker()
        log.info('Push result for task {}'.format(task_request.uuid))
        message = self._serializer.serialize(task_response)
        queue = task_request.result_queue
        result = yield from self._broker.publish_message(message, queue)
        return result

    @asyncio.coroutine
    def pop_task(self):
        """ Pop a task to be processed for the given queues.
         If no queues are passed, all queues will be tracked. """
        self._assert_broker()
        task = yield from self._broker.pop_task()
        return task

    @asyncio.coroutine
    def pop_result(self, task_request, timeout=0):
        """ lock until the result is ready """
        self._assert_broker()
        ret = yield from self._broker.pop_result(task_request, timeout)
        if ret['status'] == 'ERROR':
            log.error('Error received from task {} with param {}, {}'
                      ''.format(task_request.task_name,
                                task_request.task_args,
                                task_request.task_kwargs))
            log.error('Original {}'.format(ret['traceback']))
            exc = ret['exception']
            exc = getattr(importlib.import_module(exc['module']), exc['class'])
            raise exc(*ret['exception']['args'])
        return ret['result']

    def task(self, *args, **task_options):
        """ Decorator that transform a function to an async task """

        def wrapper(func):
            task = Task(self, func, **task_options)
            log.info('Register task {}'.format(task.name))
            self._task_registry.register(task)
            return task

        # in case it is called @task
        if args and isinstance(args[0], types.FunctionType):
            return wrapper(args[0])

        if args and isinstance(args[0], object):
            return wrapper(args[0])

        # in case it is called @task()
        return wrapper
