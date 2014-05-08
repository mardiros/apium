import sys
import argparse
import logging
import asyncio
import signal
from concurrent.futures import TimeoutError

from apium.registry import get_application
from apium.config import Configurator

log = logging.getLogger(__name__)


@asyncio.coroutine
def start_worker(queues):
    try:
        log.info('Starting Apium Worker')
        app = get_application()
        connected = yield from app.connect_broker()
        if not connected:
            log.error("Can't connect to the broker, failed to start")
            sys.exit(1)

        log.info('Apium application connected')
        queues = queues.split(',') if queues else app._working_queues

        for queue in queues:
            log.info('Create queue {}'.format(queue))
            yield from app.create_queue(queue)

        worker = app.create_worker()
        worker.start()
        yield from worker.run_forever()
    except Exception:
        log.error('Oops, something should not happen', exc_info=True)


@asyncio.coroutine
def dispose(signame, future):
    log.info('got signal {}, exiting'.format(signame))
    try:
        app = get_application()
        yield from app.stop()
        future.set_result(0)
    except Exception:
        log.error('Unexpected exception while terminating', exc_info=True)
        future.set_result(1)


def main(args=sys.argv):

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', dest='conffile', default='apium.yaml')
    subparsers = parser.add_subparsers(title='action')

    sp_worker = subparsers.add_parser('start', help='start the worker')
    sp_worker.set_defaults(func=start_worker)

    parser.add_argument('--queues', '-Q', dest='queues', action='store',
                        default=None,
                        help='comma separate queues to treat, worker will'
                             'process every queues if not provided')

    settings = parser.parse_args(args[1:])
    settings = vars(settings)

    config_uri = settings.pop('conffile')
    func = settings.pop('func')

    config = Configurator.from_yaml(config_uri)

    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.Task(func(**settings)))

    future = asyncio.Future()
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                                lambda: asyncio.async(dispose(signame,
                                                              future)))
        loop.run_until_complete(future)
        loop.stop()
        sys.exit(future.result())


if __name__ == '__main__':
    main()
