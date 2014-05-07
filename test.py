import sys
import traceback
import asyncio

from apium.registry import get_application
from apium.config import Configurator


@asyncio.coroutine
def routine(future):
    try:
        app = get_application()
        connected = yield from app.connect_broker()
        if not connected:
            print('Cannot connect to the broker')
            return

        from apium.task.sample import add, multiply, divide, Noop as noop

        result = yield from add(1, 2)
        print("1 + 2 =", result)

        result = yield from multiply(2, 16)
        print("2 * 16 = ", result)

        result = yield from divide(8, 2)
        print("8 / 2 = ", result)

        result = yield from noop(2)
        print (result)

    except Exception as exc:
        traceback.print_exc()
    finally:
        try:
            yield from app.disconnect_broker()
        except Exception as exc:
            traceback.print_exc()
        future.done()
        yield from asyncio.sleep(.2)
        import sys
        sys.exit(0)


def main(argv=sys.argv):

    Configurator.from_yaml(argv[1])
    future = asyncio.Future()
    loop = asyncio.get_event_loop()
    loop.call_soon(asyncio.Task(routine(future)))
    loop.run_until_complete(future)


if __name__ == '__main__':
    main()
