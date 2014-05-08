import logging

try:
    import simplejson as json
except ImportError:
    import json

from zope.interface import implementer
from apium.interfaces import ISerializer

log = logging.getLogger(__name__)


@implementer(ISerializer)
class Serializer:

    name = 'json'

    def serialize(self, data):
        """ Serialize data to bytes"""
        log.debug('<<< %s' % data)
        return json.dumps(data).encode('utf-8')

    def deserialize(self, bytes):
        """ Deserialize bytes to data """
        log.debug('>>> %s' % bytes)
        return json.loads(bytes.decode('utf-8'))
