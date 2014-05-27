"""
Exemple of tasks declaration.

"""

import time
import asyncio
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
class noop:
    def __call__(self, sleep_time):
        time.sleep(sleep_time)
        return sleep_time


@task(name='aionoop')
def aionoop(self, sleep_time):
    yield from asyncio.sleep(sleep_time)
    return sleep_time
