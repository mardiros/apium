import sys
import traceback
import asyncio

from apium.registry import get_driver
from apium.config import Configurator

from apium.task import sample


@asyncio.coroutine
def routine(future):
    try:
        app = get_driver()
        connected = yield from app.connect_broker()
        if not connected:
            print('Cannot connect to the broker')
            return


        result = yield from sample.add(1, 2)
        print("1 + 2 =", result)

        result = yield from sample.multiply(2, 16)
        print("2 * 16 = ", result)

        result = yield from sample.divide(8, 2)
        print("8 / 2 = ", result)

        result = yield from sample.noop(1, task_options={'timeout': 2})
        print ("wait for", result, "seconds")
        result = yield from sample.aionoop(2, task_options={'timeout': 1})
        print (result)
    except Exception as exc:
        traceback.print_exc()
    finally:
        try:
            yield from app.disconnect_broker()
        except Exception as exc:
            traceback.print_exc()
        future.set_result(None)


def main(argv=sys.argv):

    Configurator.from_yaml(argv[1])
    future = asyncio.Future()
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.Task(routine(future)))
    loop.run_until_complete(future)
    loop.stop()


if __name__ == '__main__':
    main()
