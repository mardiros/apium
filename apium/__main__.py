import sys
import argparse
import logging
import asyncio
import signal
from concurrent.futures import TimeoutError

from apium.registry import get_driver
from apium.config import Configurator

log = logging.getLogger(__name__)


@asyncio.coroutine
def start_worker(queues):
    try:
        log.info('Starting Apium Worker')
        driver = get_driver()
        connected = yield from driver.connect_broker()
        if not connected:
            log.error("Can't connect to the broker, failed to start")
            sys.exit(1)

        driver.attach_signals()

        log.info('Apium driver connected')
        queues = queues.split(',') if queues else driver._working_queues

        for queue in queues:
            log.info('Create queue {}'.format(queue))
            yield from driver.create_queue(queue)

        worker = driver.create_worker()
        worker.start()
        yield from worker.run_forever()
    except Exception:
        log.error('Oops, something should not happen', exc_info=True)


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

    asyncio.async(func(**settings))
    get_driver().run_forever()


if __name__ == '__main__':
    main()
