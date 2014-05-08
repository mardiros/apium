"""

Apium interfaces.

Apium use zope.interface to configure different broker, serializer, workers.

"""

from zope.interface import (
    Attribute,
    Interface,
    )


class IApium(Interface):
    """ Apium application container """

    settings = Attribute("""A dict like object that store application settings
                         """)

    def get_task(self, task_name):
        """ Get the task from it's name.
        The tasks must be registred previously.
        """

    def get_queue(self, task_name):
        """ Get the queue for the given task name. """

    def connect(self, *args, **kwargs):
        """ Connect to the broker server. """

    def disconnect(self, *args, **kwargs):
        """ Disconnect to the broker server. """

    def create_queue(self, queue):
        """ create a working queue where tasks will be pushed """

    def create_worker(self):
        """ Create the worker that will process the given queues """

    def push_task(self, task_request):
        """ Create a queue wich is used to push the result. """

    def push_result(self, task_request, task_response):
        """ Push the result in the result_queue from the task_resquest """

    def pop_task(self):
        """ Pop a task to be processed for the given queues.
         If no queues are passed, all queues will be tracked. """

    def pop_result(self, task_request):
        """ Lock until the result is ready, then return the result """

    def task(self, *args, **task_options):
        """ Decorator to declare a task.
        It can decore a function or a class that declare a __call__ method.

        ::

            @app.task
            def add(a, b):
                return a + b


            @app.task
            class multiply(a, b):
                def __call__(self, a, b):
                    return a * b

        It can also be called to set some options on the task.

        ::

            @app.task(name='divide')
            class Division(object):
                def __call__(self, a, b):
                    return a / b

        """


class IBroker(Interface):
    """ Mediator for tasks treatment, connect to a broker. """

    def __init__(self, application):
        """ Build the broker for the given application """

    def connect(self, *args, **kwargs):
        """ Connect to the broker server. """

    def disconnect(self, *args, **kwargs):
        """ Disconnect to the broker server. """

    def create_queue(self, queue):
        """ create a working queue where tasks will be pushed """

    def delete_queue(self, queue):
        """ delete working queues """

    def push_task(self, task_request):
        """ Push the async result in the queue. """

    def push_result(self, task_request):
        """ Push the result in the created queue. """

    def pop_task(self):
        """ Pop a task to be processed for the given queues.
         If no queues are passed, all queues will be tracked. """

    def pop_result(self, task_request, timeout=None):
        """ Pop the result for the async result,
        if a timeout is given and is attempt, raise a Timeout error"""


class IWorker(Interface):
    """ A process or a pool of process that treat tasks asynchronously.
    The worker is connected toe the broker, as a subscriber of task. """

    name = Attribute("Name of the worker")

    def __init__(self, size, max_tasks_per_child=None):
        """ Create the worker with size parallel "process"

        process is an implementation details. it can be process, thread,
        greenlet...
        """

    def start(self):
        """ Start the worker """

    def stop(self):
        """ Stop the worker """

    def join(self):
        """ Join the processing pool """

    def process(self, task):
        """ Process the task """


class ISerializer(Interface):
    """ Serialize tasks in order to wrap them in the broker. """

    name = Attribute("The name of the serializer")

    def serialize(self, task):
        """ Take a ITask and return bytes """

    def deserialize(self, bytes):
        """
        Take bytes and return an ITask

        :raises: ValueError in case bytes cannot be deserialized
        """
