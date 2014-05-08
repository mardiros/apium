import sys
import types
import logging
import asyncio
import traceback
from collections import defaultdict
from uuid import uuid4

from .. import registry

log = logging.getLogger(__name__)


class TaskRegistry(object):
    """ Default implementation of the task registry """

    def __init__(self):
        self._registry = {}
        self.default_queue = '#master'
        self.queues = defaultdict(list)

    def register(self, task):
        """ Register a task """

        if task.name in self._registry:
            raise RuntimeError('Task {} is already registered'
                               ''.format(task.name))
        if task.queue:
            self.queues[task.queue].append(task.name)
        else:
            if self.get_queue(task.name) not in self.queues:
                self.queues[self.default_queue].append(task.name)

        self._registry[task.name] = task

    def get(self, task_name):
        """
        Get the task from it's name.
        The tasks must be registred previously.
        """
        try:
            return self._registry[task_name]
        except KeyError:
            raise RuntimeError('Task {} is not registered'.format(task_name))

    def configure_queues(self, default_queue='#master', queues=None):
        self.default_queue = default_queue
        if queues:
            for queue, tasks in queues.items():
                self.queues[queue].extend(tasks)

    def get_queue(self, task_name):
        """
        Get the queue for the given task name
        """
        for name, queue in self.queues.items():
            if task_name in queue:
                return name
        return self.default_queue


class TaskRequest:
    """ Represent a task instance to run """

    def __init__(self, application, task_name, task_args, task_kwargs,
                 uuid=None, ignore_result=None,
                 result_queue=None):
        self._app = application
        self.uuid = uuid or str(uuid4())
        self.task_name = task_name
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        self.result_queue = result_queue or self._app.get_result_queue()
        self.ignore_result = ignore_result

    @asyncio.coroutine
    def get(self, timeout=None):
        """
        Return the result of the task or the result of the chained tasks in
        case some callback have been attached.

        :param timeout: timeout for the tasks. if None, the default timeout
            of the TaskRequest will be used. The default timeout is the
            timeout attribute of the tasks
        :type timeout: float

        :return: the result of the task
        """
        result = yield from self._app.pop_result(self, timeout)
        return result

    def to_dict(self):
        return {'uuid': self.uuid,
                'ignore_result': self.ignore_result,
                'result_queue': self.result_queue,
                'task_name': self.task_name,
                'task_args': self.task_args,
                'task_kwargs': self.task_kwargs,
                }

    def __str__(self):
        return '<TaskRequest {}>'.format(self.uuid)


class TaskResponse:

    def __init__(self, uuid, status, result=None,
                 exception=None, tracback=None):
        self.uuid = uuid
        self.status = status
        self.result = result
        self.exception = exception
        self.traceback = traceback

    def to_dict(self):
        ret = {'uuid': self.uuid,
               'status': self.status,
               }
        if self.status == 'DONE':
            ret['result'] = self.result
        elif self.status == 'ERROR':
            ret['exception'] = {'module': getattr(self.exception, '__module__',
                                                  '__builtin__'),
                                'class': exc.__class__.__name__,
                                'args': exc.args,
                                }
            ret['traceback'] = traceback.format_exc().strip()
        return ret


class Task:
    ignore_result = False
    queue = None
    timeout = None

    @property
    def name(self):
        if not self._name:
            self._name = '{}.{}'.format(self.method.__module__,
                                        self.method.__name__)
        return self._name

    def __init__(self, application, method, **kwargs):
        self._app = application
        self.method = method
        if 'ignore_result' in kwargs:
            self.ignore_result = kwargs['ignore_result']
        if 'timeout' in kwargs:
            self.timeout = kwargs['timeout']
        self._name = kwargs.get('name', None)

    @asyncio.coroutine
    def __call__(self, *args, **kwargs):
        ignore_result = self.ignore_result
        timeout = self.timeout
        if 'task_options' in kwargs:
            task_options = kwargs.pop('task_options')
            ignore_result = task_options.get('ignore_result', ignore_result)
            timeout = task_options.get('timeout', timeout)

        request = TaskRequest(self._app, self.name, args, kwargs,
                              ignore_result=ignore_result)
        yield from self._app.push_task(request)
        if ignore_result:
            return
        result = yield from request.get(timeout)
        return result

    def excecute(self, *args, **kwargs):
        """ execute the wrapped method """
        return (self.method(*args, **kwargs)  # function decorated
                if isinstance(self.method, types.FunctionType)
                else self.method()(*args, **kwargs)  # class decorated
                )

    def __str__(self):
        return '<task {}>'.format(self.name)


def execute_task(task_name, uuid, args, kwargs):
    """ Glue function that can be pickle.
    Python cannot easily pickle class method, that why the ITaskRegistry cannot
    be used directly.
    """
    application = registry.get_application()
    task_to_run = application.get_task(task_name)
    log.info('Executing task {}'.format(task_name))
    log.debug('with param {}, {}'.format(args, kwargs))
    try:
        ret = TaskResponse(uuid, 'DONE',
                           task_to_run.excecute(*args, **kwargs))
    except Exception as exc:
        log.error('Error {} while running task {} with param {}, {}'
                  ''.format(exc, task_name, args, kwargs))
        ret = TaskResponse(uuid, 'ERROR',
                           exception=exc,
                           traceback=sys.exc_info[2])

    ret = ret.to_dict()
    log.info('task {} executed'.format(task_name))
    log.debug('task returns {}'.format(ret))
    return ret
