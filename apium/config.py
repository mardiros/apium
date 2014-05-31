"""
Load the apium configuration
"""
import asyncio
import signal
import importlib
import logging
from logging.config import dictConfig

import venusian
from yaml import load

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from . import registry

log = logging.getLogger(__name__)


def _import(path):
    if isinstance(path, str):
        left, right = path.rsplit('.', 1)
        path = getattr(importlib.import_module(left), right)
    return path


class YamlConfig(object):

    def __init__(self, filename):
        """
        Load the apium configuration
        """
        with open(filename) as stream:
            self._config = load(stream, Loader=Loader)

    def get(self, key, default=None, separator='.'):
        """
        Get a setting from the configuration
        """
        key = key.split(separator)
        value = self._config
        try:
            for k in key:
                value = value[k]
            return value
        except KeyError:
            return default


class Configurator(object):

    def __init__(self,
                 broker_url='amqp://localhost',
                 serializer='json',
                 max_workers=None,
                 task_scan=None,
                 result_exchange='#apium-{hostname}-{pid}',
                 # Other interfaces in the registry
                 worker='apium.worker.process.Worker',
                 driver='apium.driver.Driver',
                 routes=None,
                 logging=None
                 ):

        if logging:
            dictConfig(logging)

        self.settings = {'broker_url': broker_url,
                         'max_workers': max_workers,
                         'result_exchange': result_exchange}
        self.routes = routes or {}
        self.task_scan = task_scan or []
        broker = broker_url.split(':', 1).pop(0).split('+').pop(0)
        self.implementations = {
            'IBroker': 'apium.broker.{}.Broker'.format(broker),
            'IDriver': driver,
            'ISerializer': 'apium.serializer.{}.Serializer'.format(serializer),
            'IWorker': worker,
            }

    @classmethod
    def from_yaml(cls, filename):
        """
        Load the configuration from a configuration file
        """
        config = YamlConfig(filename)
        kwargs = {'broker_url': config.get('apium.broker.url', None),
                  'task_scan': config.get('apium.scan', None),
                  'serializer': config.get('apium.serializer', None),
                  'max_workers': config.get('apium.worker.max_workers', None),
                  # Other interfaces in the registry
                  'worker': config.get('apium.registry.IWorker'),
                  'driver': config.get('apium.registry.IDriver'),
                  'result_exchange': config.get('apium.result_exchange'),
                  'logging': config.get('logging'),
                  'routes': config.get('apium.routes'),
                  }

        conf = cls(**{key: val for key, val in kwargs.items()
                      if val is not None})

        for key, val in config.get('apium.registry', {}).items():
            cls.implementations[key] = val
        conf.end()

        return conf

    def end(self):
        """ Apply the configuration in driver registry. """

        for key, val in self.implementations.items():
            registry.register(_import(val))

        driver = registry.get_driver()
        driver.settings.update(self.settings)
        driver.configure_queues(**self.routes)

        scanner = venusian.Scanner(driver=driver)
        for mod in self.task_scan:
            scanner.scan(importlib.import_module(mod))


@asyncio.coroutine
def dispose(signame):
    log.info('got signal {}, exiting'.format(signame))
    try:
        driver = get_driver()
        yield from driver.stop()
    except Exception:
        log.error('Unexpected exception while terminating', exc_info=True)


@asyncio.coroutine
def aioincludeme(config):
    Configurator.from_yaml(config.registry.settings['apium.config'])
    driver = registry.get_driver()
    connected = yield from driver.connect_broker()
    if not connected:
        raise Exception('Cannot connect to the broker')

    print('*'*80)
    loop = asyncio.get_event_loop()
    print(id(loop))
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                                lambda: asyncio.async(dispose(signame)))
    print('*'*80)
