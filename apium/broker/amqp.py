import asyncio
import logging
import uuid

from zope.interface import implementer

import aioamqp
from aioamqp.exceptions import (AmqpClosedConnection,
                                ChannelClosed,
                                ConsumerCancelled)
from apium import registry
from apium.interfaces import IBroker, ISerializer

log = logging.getLogger(__name__)


@implementer(IBroker)
class Broker(object):
    """ IBroker implementation for AMQP """

    def __init__(self, driver):
        """ Build the broker for the given driver """
        self._driver = driver
        self._task_queue = asyncio.Queue(maxsize=1)
        self._serializer = registry.get(ISerializer)()
        self._start_consuming = False
        self._start_consuming_task = False
        self._start_consuming_result = False
        self._results = {}
        self._protocol = None
        self._channel = None
        self._consumer_tags = []
        self.connected = False

    @asyncio.coroutine
    def connect(self, url):
        """ Connect to the broker server.
        """
        try:
            self._protocol = yield from aioamqp.from_url(url)
            self._channel = yield from self._protocol.channel()
            self.connected = True
        except Exception:
            import traceback
            traceback.print_exc()

    @asyncio.coroutine
    def disconnect(self):
        """ Disconnect to the broker server. """
        self.connected = False
        log.info('basic cancel')
        for tag in self._consumer_tags:
            yield from self._channel.basic_cancel(tag)

        log.info('closing channel')
        yield from self._channel.close()
        self._channel = None
        self._protocol.stop()
        self._protocol = None

    @asyncio.coroutine
    def create_queue(self, queue):
        """ create a working queue where tasks will be pushed.
        To K.I.S.S., use direct exchange to queue.
        e.g. 1 exchange per queue with the same name.
        """
        log.info('Creating echange {}'.format(queue))
        yield from self._channel.exchange(queue, 'direct')
        log.info('Creating queue {}'.format(queue))
        yield from self._channel.queue(queue, durable=True)
        log.info('Binding queue {}'.format(queue))
        yield from self._channel.queue_bind(queue, queue, queue)
        log.info('Queue {} created'.format(queue))

    @asyncio.coroutine
    def delete_queue(self, queue):
        """ delete working queues """
        log.info('Deleting exchange {}'.format(queue))
        try:
            yield from self._channel.exchange_delete(queue, no_wait=False)
        except Exception:
            log.exception('Unmanaged exception while deleting exchange')
        log.info('Deleting queue {}'.format(queue))
        try:
            yield from self._channel.queue_delete(queue, no_wait=False)
        except Exception:
            log.exception('Unmanaged exception while deleting queue {}'
                          ''.format(queue))

    @asyncio.coroutine
    def publish_message(self, message, queue):
        """ publish a message in a queue. """
        try:
            yield from self._channel.publish(message,
                                             exchange_name=queue,
                                             routing_key=queue)
            return True
        except Exception:
            log.error('Unexpected error while pushing message', exc_info=True)
            return False

    @asyncio.coroutine
    def pop_task(self):
        """ Pop a task to be processed for the given queues.
        If no queues are passed, all queues will be tracked. """
        try:
            if not self._start_consuming_task:
                self._start_consuming_task = True
                yield from self._subscribe_task_queues()

            task = yield from self._task_queue.get()
        except Exception:
            log.error('Unexpected error while poping task', exc_info=True)
            raise
        return task

    @asyncio.coroutine
    def pop_result(self, task_request, timeout=None):

        if not self._start_consuming_result:
            self._start_consuming_result = True
            yield from self._subscribe_result_queue()

        future = asyncio.Future()
        loop = asyncio.get_event_loop()
        self._results[task_request.uuid] = future
        try:
            result = yield from asyncio.wait_for(future, timeout)
        except TimeoutError:
            future.cancel()
            del self._results[task_request.uuid]
            raise
        return result

    @asyncio.coroutine
    def _subscribe_result_queue(self):

        queue = self._driver.get_result_queue()
        log.info('basic consume {}'.format(queue))
        consumer_tag = 'result-{}'.format(queue)
        self._consumer_tags.append(consumer_tag)
        yield from self._channel.basic_consume(queue, consumer_tag,
                                               no_wait=False)
        asyncio.async(self._consume_queue(consumer_tag))

    @asyncio.coroutine
    def _subscribe_task_queues(self):

        for queue in self._driver._working_queues:
            log.info('basic consume {}'.format(queue))
            consumer_tag = 'task-{}'.format(queue)
            self._consumer_tags.append(consumer_tag)
            yield from self._channel.basic_consume(queue, consumer_tag,
                                                   no_wait=False)
            loop = asyncio.get_event_loop()
            asyncio.async(self._consume_queue(consumer_tag))

    @asyncio.coroutine
    def _consume_queue(self, consumer_tag):

        while self._channel:
            try:
                (consumer_tag,
                 delivery_tag,
                 message) = yield from self._channel.consume(consumer_tag)
                log.debug('Consumer {} received {} ({})'
                          ''.format(consumer_tag, message, delivery_tag))
                message = self._serializer.deserialize(message)
                if consumer_tag.split('-', 1).pop(0) == 'task':
                    log.debug('Pushing task in the task queue')
                    yield from self._task_queue.put(message)
                else:
                    try:
                        self._results[message['uuid']].set_result(message)
                        log.debug('Result for {} pushed in the result dict'
                                  ''.format(message['uuid']))
                    except KeyError:
                        log.warn('Result arrived to late')

                # XXX ack_late
                yield from self._channel.basic_client_ack(delivery_tag)

            except (ChannelClosed, ConsumerCancelled) as exc:
                if not self.connected:
                    break
                log.warning('Consumer has been closed, open a new channel')
                # reconnect the channel
                self._channel = yield from self._protocol.channel()
                if self._start_consuming_task:
                    yield from self._subscribe_task_queues()
                if self._start_consuming_result:
                    yield from self._subscribe_result_queue()

            except Exception:
                log.error('Unexpected exception while reveicing message',
                          exc_info=True)
