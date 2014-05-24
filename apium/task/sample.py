"""
Exemple of tasks declaration.

"""


import time
from apium import registry
from apium.task import task


@task()
def add(a, b):
    return a + b


@task()
def multiply(a, b):
    return a * b


@task()
class divide:
    def __call__(self, a, b):
        return a / b


@task(name='noop')
class Noop:
    def __call__(self, sleep_time):
        time.sleep(sleep_time)
