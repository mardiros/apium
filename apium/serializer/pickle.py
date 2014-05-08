import logging
import pickle

from zope.interface import implementer
from apium.interfaces import ISerializer


log = logging.getLogger(__name__)


@implementer(ISerializer)
class Serializer:

    name = 'pickle'

    def serialize(self, data):
        """ Serialize data to bytes"""
        log.debug('<<< %s' % data)
        return pickle.dumps(data)

    def deserialize(self, bytes):
        """ Deserialize bytes to data """
        log.debug('>>> %s' % bytes)
        return pickle.loads(bytes)
