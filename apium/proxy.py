"""


"""
import asyncio

from .registry import get_driver



class Proxy:
    def __getattr__(self, name):
        if name == '__venusian_callbacks__':  # Venusian scan must ignore me
            raise AttributeError(name)
        return _Method(name)


class _Method:
    # some magic to bind an XML-RPC method to an RPC server.
    # supports "nested" methods (e.g. examples.getStateName)
    def __init__(self, name):
        self.__name = name

    def __getattr__(self, name):
        return _Method('%s.%s' % (self.__name, name))

    @asyncio.coroutine
    def __call__(self, *args, **kwargs):
        task = get_driver().get_task(self.__name)
        return (yield from task(*args, **kwargs))


apium = Proxy()