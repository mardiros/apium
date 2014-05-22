Apium
=====

Getting Started
---------------

Apium is a job execution for asyncio inspired by Celery,
but for the new python 3.4 asyncio minimal framework.

It is currently experimental.

Requirements
------------

* Python 3.4


Installation
------------


As its still experimental, no release has been made yet,
the simple way to git it a try is to build a virtualenv
and get the code on github.

::

    pyvenv venv34
    source venv34/bin/activate
    pip install -e "git+https://github.com/mardiros/apium.git#egg=apium"



start the apium server ::

    apium -f apium.yaml start


run the publisher ::

    python test.py apium.yaml


.. note::

    apium.yaml can be copy from apium.sample.yaml,
    you should edit it to configure the access of your amqp
