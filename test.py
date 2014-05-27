import sys
import traceback
import asyncio

from apium.registry import get_driver
from apium.config import Configurator
from apium.proxy import apium


@asyncio.coroutine
def routine(future, config):
    try:
        Configurator.from_yaml(config)
        yield from get_driver().connect_broker()
        get_driver().attach_signals()

        result = yield from apium.apium.task.sample.add(1, 2)
        print("1 + 2 =", result)

        result = yield from apium.dotted.multiply(2, 16)
        print("2 * 16 = ", result)

        result = yield from apium.apium.task.sample.divide(8, 2)
        print("8 / 2 = ", result)

        result = yield from apium.noop(1, task_options={'timeout': 2})
        print ("wait for", result, "seconds")
        result = yield from apium.aionoop(2, task_options={'timeout': 1})
        print (result)
    except Exception as exc:
        traceback.print_exc()
    finally:
        try:
            yield from get_driver().disconnect_broker()
        except Exception as exc:
            traceback.print_exc()
        future.set_result(None)


def main(argv=sys.argv):

    future = asyncio.Future()
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.Task(routine(future, argv[1])))
    loop.run_until_complete(future)
    loop.stop()


if __name__ == '__main__':
    main()
