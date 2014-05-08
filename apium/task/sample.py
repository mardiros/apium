"""
Exemple of tasks declaration.

"""


import time
from apium import registry

app = registry.get_application()


@app.task
def add(a, b):
    return a + b


@app.task()
def multiply(a, b):
    return a * b


@app.task
class divide:
    def __call__(self, a, b):
        return a / b


@app.task(name='apium.task.sample.noop')
class Noop:
    def __call__(self, sleep_time):
        time.sleep(sleep_time)
