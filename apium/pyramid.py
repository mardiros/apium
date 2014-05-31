"""
Configuration for the pyramid_asyncio module.
"""

import asyncio

from .config import Configurator
from .registry import get_driver


@asyncio.coroutine
def at_exit():
    yield from get_driver().stop()


@asyncio.coroutine
def includeme(config):
    settings = config.get_settings()
    Configurator.from_yaml(settings['apium.config'])
    driver = get_driver()
    connected = yield from driver.connect_broker()
    if not connected:
        log.error('Cannot connect to the broker')
        return
    config.add_exit_handler(at_exit)
