import logging
import asyncio
from concurrent.futures import ProcessPoolExecutor

from zope.interface import implementer
from apium.interfaces import IWorker
from apium.application import registry
from apium.task import execute_task, TaskRequest

log = logging.getLogger(__name__)


@implementer(IWorker)
class Worker:

    name = 'process'

    def __init__(self, application):
        self._app = application
        self._pool = None
        self._pool_kwargs = {'max_workers': application.settings['max_workers']}

    def start(self):
        """ Start the worker pool """
        log.info('Start the apium processing pool')
        self._pool = ProcessPoolExecutor(**self._pool_kwargs)

    def stop(self):
        """ Stop the worker pool """
        log.info('Stop the apium processing pool')
        try:
            self._pool.shutdown(wait=True)
        except Exception:
            log.error('Unexpected Error while shutting done the pool',
                      exc_info=True)
        log.info('Apium processing pool stopped')

    @asyncio.coroutine
    def run_forever(self):
        """ Consume tasks """
        log.info('Worker is ready to accept tasks')
        while True:
            try:
                log.debug('Worker wait for a new task')
                task_dict = yield from self._app.pop_task()
            except Exception:
                log.error('Unexpected error while retrieving task',
                          exc_info=True)
                yield from asyncio.sleep(0)
                continue
            try:
                log.info('Received task {task_name} {uuid}'
                         ''.format(**task_dict))
                yield from self.process(**task_dict)
            except Exception as exc:
                log.error('Error %s while running task %r' % (exc, task_dict),
                          exc_info=True)

    @asyncio.coroutine
    def process(self, uuid, result_queue, task_name, task_args, task_kwargs,
                ignore_result):
        """ Process the task """
        async = TaskRequest(self._app, task_name, task_args, task_kwargs,
                            uuid=uuid, ignore_result=ignore_result,
                            result_queue=result_queue)
        loop = asyncio.get_event_loop()
        result = yield from loop.run_in_executor(self._pool,
                                                 execute_task,
                                                 task_name, uuid,
                                                 task_args, task_kwargs)
        if not ignore_result:
            log.info('Pushing result for task {} {}'
                     ''.format(task_name, uuid))
            yield from self._app.push_result(async, result)
